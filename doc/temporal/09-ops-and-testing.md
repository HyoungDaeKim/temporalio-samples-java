# 운영과 테스트 — Schedule / Replay / Test

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 26. `Schedule` — Workflow를 주기적으로 실행 (Cron의 상위 호환)

### 역할
Workflow를 **일정에 따라 자동 실행**시키는 서버 측 스케줄러. `WorkflowOptions.setCronSchedule(...)`(§6)은 "워크플로 자신이 cron으로 재시작되는" 레거시 방식인 반면, `Schedule`은 **별도 Schedule 엔티티**로 등록되어 **일시정지/수동 트리거/정책 변경/통계 조회**가 가능하다. Cron workflow를 교체하는 1세대 피처.

### 전체 흐름 (`core/.../hello/HelloSchedules.java`)

```java
// 1) ScheduleClient — WorkflowClient와는 별도 (같은 service 공유)
ScheduleClient scheduleClient = ScheduleClient.newInstance(service);

// 2) 실행할 Action 정의 — "schedule이 fire되면 뭘 할지"
ScheduleActionStartWorkflow action = ScheduleActionStartWorkflow.newBuilder()
    .setWorkflowType(GreetingWorkflow.class)
    .setArguments("World")
    .setOptions(WorkflowOptions.newBuilder()
        .setWorkflowId(WORKFLOW_ID)
        .setTaskQueue(TASK_QUEUE)
        .build())
    .build();

// 3) Schedule 정의 — Action + Spec (언제 돌릴지)
Schedule schedule = Schedule.newBuilder()
    .setAction(action)
    .setSpec(ScheduleSpec.newBuilder().build())      // 처음엔 빈 spec (수동 트리거만)
    .build();

// 4) 서버에 등록 — ScheduleHandle 반환
ScheduleHandle handle = scheduleClient.createSchedule(
    SCHEDULE_ID, schedule, ScheduleOptions.newBuilder().build());

// 5) 수동 트리거
handle.trigger(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL);

// 6) Spec 업데이트
handle.update(input -> {
    Schedule.Builder b = Schedule.newBuilder(input.getDescription().getSchedule());
    b.setSpec(ScheduleSpec.newBuilder()
        .setCalendars(List.of(ScheduleCalendarSpec.newBuilder()           // 매주 금 17시
            .setHour(List.of(new ScheduleRange(17)))
            .setDayOfWeek(List.of(new ScheduleRange(5))).build()))
        .setIntervals(List.of(new ScheduleIntervalSpec(Duration.ofSeconds(5))))  // 또는 5초마다
        .build());
    b.setState(ScheduleState.newBuilder()
        .setPaused(true)                                                   // 처음엔 pause
        .setLimitedAction(true)
        .setRemainingActions(10)                                            // 10번만
        .build());
    return new ScheduleUpdate(b.build());
});

// 7) 재개
handle.unpause();

// 8) 상태 조회
ScheduleState state = handle.describe().getSchedule().getState();

// 9) 삭제
handle.delete();
```

### `ScheduleSpec` — 언제 실행할지

| 필드 | 설명 |
| --- | --- |
| `setCalendars(List<ScheduleCalendarSpec>)` | 달력 기반 — "매주 금 17시", "매월 1일" 등. cron보다 **구조화된** 표현 |
| `setIntervals(List<ScheduleIntervalSpec>)` | 고정 간격 — "N초/분/시간마다" |
| `setCronExpressions(List<String>)` | **cron 식**도 지원 — "0 17 * * 5" |
| `setSkip(List<ScheduleCalendarSpec>)` | 특정 시점 **제외** (예: 공휴일) |
| `setStartAt(Instant)` / `setEndAt(Instant)` | 시작/종료 시각 |
| `setJitter(Duration)` | 각 실행에 무작위 지연 추가 (쇄도 방지) |
| `setTimeZoneName(String)` | 타임존 (기본 UTC) |

`ScheduleCalendarSpec` 안의 `setHour`, `setMinute`, `setDayOfWeek`, `setDayOfMonth`, `setMonth`, `setYear`는 전부 `List<ScheduleRange>`로 다중 값·범위·스텝 표현 가능.

### `ScheduleOverlapPolicy` — 이전 실행이 안 끝났을 때

이전 fire의 Workflow가 아직 돌고 있을 때 다음 fire가 오면:

| 값 | 동작 |
| --- | --- |
| `SKIP` (기본) | 이번 fire를 건너뜀 |
| `BUFFER_ONE` | 하나는 대기 큐에 쌓음 (그 이상은 건너뜀) |
| `BUFFER_ALL` | 전부 큐에 쌓음 |
| `CANCEL_OTHER` | 이전 Workflow에 cancel 요청 보내고 새로 시작 |
| `TERMINATE_OTHER` | 이전 Workflow를 terminate하고 새로 시작 |
| `ALLOW_ALL` | 겹쳐도 그냥 동시에 실행 (샘플이 수동 trigger에 사용) |

### Schedule이 자동으로 주입하는 Search Attributes

Workflow가 **Schedule에 의해 시작된 경우** 두 search attribute가 자동 세팅됨:

```java
// HelloSchedules.java의 Workflow 코드에서 추출 예
Payload scheduledByIDPayload = Workflow.getInfo().getSearchAttributes()
    .getIndexedFieldsOrThrow("TemporalScheduledById");       // 어느 Schedule이 트리거했나
Payload startTimePayload = Workflow.getInfo().getSearchAttributes()
    .getIndexedFieldsOrThrow("TemporalScheduledStartTime");  // 트리거 예정 시각
```
→ 지연 여부(실제 시작 시각 - 예정 시각) 측정, 어느 Schedule에서 왔는지 추적에 사용.

### `ScheduleHandle` 주요 메서드

| 호출 | 설명 |
| --- | --- |
| `trigger(ScheduleOverlapPolicy)` | 지금 즉시 1회 실행 (spec과 무관) |
| `pause(String note)` / `unpause(String note)` | 일시정지 / 재개 (사유 기록) |
| `update(Function)` | spec·state·action 변경. 함수형으로 기존 설명 받아 수정 |
| `describe()` | 현재 Schedule 상태 조회 (최근 실행, 다음 실행 예정 시각 등) |
| `backfill(List<ScheduleBackfill>)` | 과거 시점부터 **채워 넣기** 실행 — 놓친 실행 재발행 |
| `delete()` | Schedule 삭제 (이미 시작된 Workflow는 영향 없음) |
| `listScheduleActionResult()` | 실행 이력 조회 |

### Cron workflow(`WorkflowOptions.setCronSchedule`)와의 차이

| 항목 | **CronSchedule** (레거시) | **Schedule** (권장) |
| --- | --- | --- |
| 실체 | Workflow 자체가 cron으로 재시작 | 별도 Schedule 엔티티 |
| 일시정지 | 불가 (워크플로 cancel해야 함) | `pause()` |
| 수동 트리거 | 불가 | `trigger()` |
| spec 변경 | 재생성 필요 | `update()` |
| 과거 분 채우기 | 불가 | `backfill()` |
| 중첩 정책 | 없음 (전 run 끝날 때까지 대기) | 6가지 `OverlapPolicy` |
| 서버 요구 | 모든 버전 | Temporal 1.17+ |

→ **신규 설계는 Schedule을 쓰고, 기존 CronSchedule은 유지보수 시점에 마이그레이션**이 권장되는 방향.

### 자주 하는 실수

1. **`ScheduleClient` 대신 `WorkflowClient.start(cronOptions)` 혼용** — Schedule을 쓸 거면 전부 `ScheduleClient`로. 혼용하면 둘이 서로 모른다.
2. **Overlap policy 미지정** — 기본 `SKIP`이 예기치 못한 결과 유발 (실행이 하나 길게 끌리면 그동안 전부 skip). 명시적 선택 필요.
3. **Schedule 삭제 ≠ 실행 중 Workflow 종료** — `handle.delete()`는 **앞으로의 실행만** 중단. 이미 돌고 있는 Workflow는 그대로. 필요하면 별도 cancel.
4. **`setJitter` 없이 많은 Schedule을 동일 시각에** — 같은 분에 fire되면 쇄도 발생. jitter로 분산.
5. **Schedule을 끄지 않고 Workflow 코드 배포로 변경 시도** — Schedule에 저장된 `ScheduleActionStartWorkflow`는 **Workflow Type과 인자**를 담고 있다. 코드 바뀌어도 Schedule은 안 바뀜. 변경하려면 `handle.update(...)`.

### 샘플 레퍼런스
- Schedule 전체 라이프사이클(create → trigger → update → unpause → describe → delete): `core/src/main/java/io/temporal/samples/hello/HelloSchedules.java`

> 저장소에는 Schedule 샘플이 이 1개뿐이지만, Workflow 안에서 `TemporalScheduledById`/`TemporalScheduledStartTime` search attribute를 읽는 패턴까지 포함해 거의 모든 유즈케이스가 담겨 있다.

---
## 27. `Replay` — 히스토리를 다시 돌려 결정성 검증

### 역할
저장된 Workflow 히스토리(JSON 또는 proto)를 **코드 변경 후에도 결정적으로 재생되는지** 검증하는 테스트 유틸. 라이브 Temporal 서버 없이 히스토리만으로 Workflow 코드를 돌려 **비결정성 버그를 CI에서 조기 발견**한다. `io.temporal.testing.WorkflowReplayer` 사용.

### 왜 필요한가 — 결정성 깨짐의 비싼 결과

Workflow 코드를 수정했는데 기존에 실행 중인 Workflow가 리플레이될 때 **다른 결정 경로**로 흐르면:
- `NonDeterministicException` 발생 → Workflow Task 영구 실패 → Workflow 전체 stuck
- 운영 중 발견되면 수동 복구 필요 (patch API로 분기 처리하거나 reset)

**Replay 테스트는 배포 전에 "현재 코드로 과거 히스토리를 돌려 깨지는지" 확인**해 이걸 막는다.

### 기본 사용 (`core/.../hello/HelloActivityReplayTest.java`)

```java
@Test
public void replayWorkflowExecution() throws Exception {
    // 1) 히스토리 JSON 확보 (실제 운영에선 서버/UI에서 export)
    String eventHistory = executeWorkflow(GreetingWorkflowImpl.class);

    // 2) 같은 Workflow 구현체로 리플레이 — 통과해야 함
    WorkflowReplayer.replayWorkflowExecution(eventHistory, GreetingWorkflowImpl.class);
}

@Test
public void replayWorkflowExecutionNonDeterministic() {
    try {
        // 1) 구현체 A로 히스토리 생성 (sleep 추가된 변형)
        String eventHistory = executeWorkflow(GreetingWorkflowImplTest.class);

        // 2) 구현체 B(원본)로 리플레이 — 결정 경로가 달라 터져야 정상
        WorkflowReplayer.replayWorkflowExecution(eventHistory, GreetingWorkflowImpl.class);

        Assert.fail("Should have thrown an Exception");
    } catch (Exception e) {
        assertThat(e.getMessage(),
            CoreMatchers.containsString("error=io.temporal.worker.NonDeterministicException"));
    }
}
```

### `WorkflowReplayer` 주요 메서드

| 메서드 | 설명 |
| --- | --- |
| `replayWorkflowExecution(String json, Class... types)` | JSON 문자열 히스토리로 리플레이 |
| `replayWorkflowExecution(File jsonFile, Class... types)` | JSON 파일 로드 |
| `replayWorkflowExecution(WorkflowExecutionHistory, Class...)` | 이미 파싱된 히스토리 객체 |
| `replayWorkflowExecutions(List<WorkflowExecutionHistory>, ...)` | **여러 히스토리 일괄** — CI에서 과거 N개 샘플 돌리기 |
| `replayWorkflowExecutionFromResource(String name, Class...)` | 테스트 리소스 폴더에서 로드 (CI 친화적) |

### 히스토리를 어디서 얻나

| 방법 | 용도 |
| --- | --- |
| **Temporal CLI** — `temporal workflow show -w <id> --output json > history.json` | 운영 Workflow 1건 export |
| **Web UI** — Workflow Execution 페이지의 "Download" 버튼 | 수동 분석용 |
| **Java API** — `WorkflowStub.getExecutionHistory()` / 테스트의 `TestWorkflowRule.getHistory(execution)` | 테스트에서 생성 후 즉시 리플레이 |
| **버전 관리에 체크인** — 과거 중요한 Workflow 히스토리를 테스트 리소스로 저장 → CI에서 매 PR마다 리플레이 | **운영 안정성 핵심 패턴** |

### CI에 통합하는 전형 패턴

1. 운영 환경에서 **대표성 있는** Workflow 히스토리들을 `core/src/test/resources/replay/` 등에 저장.
2. Replay 테스트에서 **파일별로** `replayWorkflowExecutionFromResource(...)` 호출.
3. Workflow 코드를 변경하는 PR이 결정성을 깨면 CI가 실패.

```java
@Test
public void replayHistoricalSamples() throws Exception {
    for (String resource : List.of("payment-v1.json", "payment-v2.json", "payment-v3.json")) {
        WorkflowReplayer.replayWorkflowExecutionFromResource(resource, PaymentWorkflowImpl.class);
    }
}
```

### 결정성을 깨는 전형적 변경

| 변경 | 영향 |
| --- | --- |
| Activity/Timer **순서** 바꾸기 | 즉시 터짐 |
| Activity/Timer/Signal 핸들러 **추가/삭제** | 과거 히스토리와 불일치 |
| `if/else` **조건 반대로** | 다른 분기로 흘러 `NonDeterministicException` |
| `Duration` 값 변경 | Timer command가 다른 duration으로 → 터짐 |
| 반복 횟수 변경 | 히스토리의 Activity 수와 불일치 |

### 안전하게 변경하는 방법 — `Workflow.getVersion`

결정성을 깨는 변경을 하려면 `Workflow.getVersion("change-id", DEFAULT, 1)` 같은 패치 API로 **분기 처리**. 과거 히스토리는 old path, 새 히스토리는 new path로 흐르게. Replay 테스트는 두 path 모두 통과하는지 검증.

### 자주 하는 실수

1. **Replay 테스트 안 돌림** — "리플레이는 운영에서 터지면 알겠지"는 매우 비싼 전략. 반드시 CI에 포함.
2. **대표 히스토리 1개만** — 분기가 많은 Workflow는 **각 경로의 히스토리** 모두 저장. 하나로는 숨은 깨짐 못 잡음.
3. **환경 의존 Workflow 코드** (`System.getenv`, 설정 로드) — Replay 환경에서 다르면 터진다. `WorkflowImplementationOptions`로 주입하거나 Activity로.
4. **`WorkflowReplayer`를 운영 코드에 섞음** — 테스트 전용. 실제 Workflow 실행은 Worker가 자동 리플레이.
5. **버전 바뀐 후 과거 히스토리 바로 제거** — 운영 중인 Workflow가 완료될 때까지는 보관. `Workflow.getVersion` 분기도 과거 Run이 다 끝나야 삭제 가능.

### 샘플 레퍼런스
- 결정적/비결정적 리플레이 둘 다 보여줌: `core/src/test/java/io/temporal/samples/hello/HelloActivityReplayTest.java`
- Interceptor가 리플레이 시에도 호출되는지 검증: `core/src/test/java/io/temporal/samples/interceptorreplaytest/InterceptorReplayTest.java`

---
## 28. `TestWorkflowRule` / `TestWorkflowEnvironment` / `TestWorkflowExtension` — 서버 없이 테스트

### 역할
**실제 Temporal 서버 없이 in-process로 Workflow를 실행**해 테스트. 수년의 Timer를 수 밀리초에 time-skip하고, Activity를 mock하고, 전체 라이프사이클을 결정적으로 재현한다. `io.temporal:temporal-testing` 의존성 포함.

### 세 가지 진입점 — 같은 엔진, 다른 API

| API | 테스트 프레임워크 | 사용 지점 |
| --- | --- | --- |
| **`TestWorkflowRule`** | JUnit 4 `@Rule` | 저장소의 거의 모든 샘플 테스트 |
| **`TestWorkflowExtension`** | JUnit 5 `@RegisterExtension` + 파라미터 주입 | `SleepForDaysJUnit5Test.java` 등 `*JUnit5Test`로 끝나는 샘플 |
| **`TestWorkflowEnvironment`** | 저수준 — Rule/Extension 안에도 들어있고 **직접 생성·수동 라이프사이클** 가능 | 복잡한 테스트, Spring Boot 통합 등 |

Rule/Extension은 `TestWorkflowEnvironment`를 감싸 **before/after 자동 관리**하는 shortcut. 커스텀 setup이 많으면 Environment를 직접 만들어 쓴다.

### Rule — JUnit 4 패턴 (`core/.../sleepfordays/SleepForDaysTest.java`)

```java
public class SleepForDaysTest {
    @Rule
    public TestWorkflowRule testWorkflowRule = TestWorkflowRule.newBuilder()
        .setWorkflowTypes(SleepForDaysImpl.class)
        .setDoNotStart(true)                   // Activity 등록 후 수동 start
        .build();

    @Test(timeout = 8000)
    public void testSleepForDays() {
        SendEmailActivity activities = mock(SendEmailActivity.class);
        testWorkflowRule.getWorker().registerActivitiesImplementations(activities);
        testWorkflowRule.getTestEnvironment().start();

        SleepForDaysWorkflow workflow = testWorkflowRule.getWorkflowClient()
            .newWorkflowStub(SleepForDaysWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setTaskQueue(testWorkflowRule.getTaskQueue())    // Rule이 만든 TaskQueue 사용
                    .build());

        WorkflowClient.start(workflow::sleepForDays);

        // ⏩ 핵심 — 90일을 수 밀리초에 time-skip
        testWorkflowRule.getTestEnvironment().sleep(Duration.ofDays(90));

        verify(activities, times(4)).sendEmail(anyString());   // 30일마다 호출된 걸 검증
    }
}
```

### Extension — JUnit 5 패턴 (`core/.../sleepfordays/SleepForDaysJUnit5Test.java`)

```java
public class SleepForDaysJUnit5Test {
    @RegisterExtension
    public TestWorkflowExtension testWorkflowRule = TestWorkflowExtension.newBuilder()
        .registerWorkflowImplementationTypes(SleepForDaysImpl.class)
        .setDoNotStart(true)
        .build();

    @Test
    @Timeout(8)
    public void testSleepForDays(
            TestWorkflowEnvironment testEnv,
            Worker worker,
            SleepForDaysWorkflow workflow) {           // ← 자동 주입
        SendEmailActivity activities = mock(SendEmailActivity.class);
        worker.registerActivitiesImplementations(activities);
        testEnv.start();

        WorkflowClient.start(workflow::sleepForDays);
        testEnv.sleep(Duration.ofDays(90));
        ...
    }
}
```
JUnit 5 extension은 **메서드 파라미터로 Env/Worker/Workflow stub을 자동 주입** — 보일러플레이트가 줄어듦.

### `TestWorkflowRule.Builder` 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setWorkflowTypes(Class...)` | 등록할 Workflow 구현 클래스들 |
| `setActivityImplementations(Object...)` | Activity 인스턴스 등록 (mock도 가능) |
| `setWorkflowClientOptions(...)` | 커스텀 `DataConverter`, Interceptor 주입 (`core/.../payloadconverter/CryptoPayloadConverterTest.java`) |
| `setWorkerFactoryOptions(...)` | `WorkerInterceptors` 등 (`core/.../tracing/TracingTest.java`) |
| `setWorkerOptions(...)` | Worker별 세부 옵션 |
| `setNamespace(String)` | 테스트용 namespace (기본 자동 생성) |
| `setDoNotStart(true)` | **Environment를 자동 start하지 않음** — 테스트 안에서 추가 setup 후 `getTestEnvironment().start()` 호출 |
| `setUseExternalService(true)` | **실제 Temporal 서버 사용** (통합 테스트 전환 토글) |
| `setNexusEndpoint(...)` / `setNexusService(...)` | Nexus 테스트 (`core/.../nexus/caller/CallerWorkflowTest.java`) |

### `TestWorkflowEnvironment` 핵심 메서드 — "시간 조작"

| 메서드 | 설명 |
| --- | --- |
| `start()` | WorkerFactory start |
| `shutdown()` | 종료 |
| `sleep(Duration)` | **time-skip** — Workflow Timer가 전부 즉시 fire. 실제 시간 대기 없음 |
| `currentTimeMillis()` | Environment 내부 가상 시각 (실제 시각과 무관) |
| `getWorkflowClient()` | 테스트용 WorkflowClient |
| `getWorkflowService()` | 저수준 service stub |
| `registerSearchAttribute(...)` | 검색 attribute 미리 등록 (실제 서버에선 CLI로 등록) |
| `getDiagnostics()` | 실패 시 디버깅용 상세 정보 |

### Mock Activity와 Mock Nexus

**Activity Mock** — Mockito로 Activity interface를 mock하고 Rule에 등록:
```java
GreetingActivities activities = mock(GreetingActivities.class);
testWorkflowRule.getWorker().registerActivitiesImplementations(activities);
when(activities.compose(any())).thenReturn("mocked");
```

**Nexus Mock** — `core/.../nexus/caller/NexusServiceMockTest.java` 참고.

### "실제 시간" vs "가상 시간" — 가장 중요한 차이

| 상황 | TestEnv의 `sleep` | `Thread.sleep` |
| --- | --- | --- |
| Workflow 안 `Workflow.sleep(30일)` | 수 밀리초에 fire | 30일 실제 대기 (금지) |
| 외부에서 "N초 뒤에 검증" | **`testEnv.sleep(d)`** 호출 | `Thread.sleep` 가능하지만 느림 |

→ **테스트에서 `Workflow.sleep(Duration.ofDays(30))`을 쓰는 Workflow를 검증하려면 반드시 `testEnv.sleep(d)`로 시간 전진**. 실제 30일 안 기다림.

### "서버 없이"의 한계 — Cron format 등

TestWorkflowRule의 in-process 서비스는 **실제 Temporal 서버와 100% 호환되지 않는다**. 샘플 주석도 명시:

> *"Unfortunately the supported cron format of the Java test service is not exactly the same as the temporal service. For example `@every` is not supported by the unit testing framework."* — `HelloCronTest.java`

→ 복잡한 cron, 최신 서버 피처, 멀티 네임스페이스는 **`setUseExternalService(true)`로 전환**해 실제 서버로 테스트하는 쪽이 안전.

### Spring Boot 테스트는 조금 다름

`springboot/*`와 `springboot-basic/*`의 테스트는 `@SpringBootTest`와 Spring 전용 `TestTemporalBoot` 설정을 조합. `TestWorkflowEnvironment`를 Spring Bean으로 주입. 예: `springboot/.../HelloSampleTest.java`.

### 자주 하는 실수

1. **`Thread.sleep`로 Timer 기다림** — 실제 30일 대기. 반드시 `testEnv.sleep(d)`.
2. **`setDoNotStart(true)` 없이 Rule을 만들고 Activity mock 등록** — Rule이 이미 start해버려 Workflow가 등록 시점 전에 실행될 수 있음. mock 등록이 필요하면 `setDoNotStart(true)` + `testEnv.start()`.
3. **`testWorkflowRule.getTaskQueue()` 안 쓰고 하드코딩** — Rule이 매 테스트마다 **다른 TaskQueue를 생성**하므로, 하드코딩하면 다른 테스트가 영향 줄 수 있다. 항상 `getTaskQueue()`.
4. **TestEnv를 공유** (static) — 테스트 간 상태 격리 깨짐. Rule/Extension은 매 테스트마다 새 Env를 만든다.
5. **"테스트는 통과했는데 운영에서 cron 실패"** — in-process 서비스 한계. 서버 의존 피처는 `setUseExternalService(true)` 통합 테스트로 보완.
6. **mock Activity의 상호작용 횟수 검증 시점을 잘못 잡기** — `testEnv.sleep(d)` **후에** `verify(...)` 호출. 그 전에는 호출이 아직 일어나지 않았을 수 있음.

### 샘플 레퍼런스
- JUnit 4 Rule 기본 + time-skip: `core/src/test/java/io/temporal/samples/sleepfordays/SleepForDaysTest.java`
- JUnit 5 Extension + 파라미터 주입: `core/src/test/java/io/temporal/samples/sleepfordays/SleepForDaysJUnit5Test.java`
- Activity mock: `core/src/test/java/io/temporal/samples/hello/HelloCronTest.java`
- Nexus mock: `core/src/test/java/io/temporal/samples/nexus/caller/NexusServiceMockTest.java`, `CallerWorkflowMockTest.java`
- `setWorkflowClientOptions`로 DataConverter 주입: `core/src/test/java/io/temporal/samples/payloadconverter/CryptoPayloadConverterTest.java`
- `setWorkerFactoryOptions`로 Interceptor 주입: `core/src/test/java/io/temporal/samples/tracing/TracingTest.java`
- Nexus endpoint 바꿔치기: `core/src/test/java/io/temporal/samples/nexus/caller/CallerWorkflowTest.java`
- Spring Boot 통합: `springboot/src/test/java/io/temporal/samples/springboot/HelloSampleTest.java`, `HelloSampleTestMockedActivity.java`
- JUnit 4와 JUnit 5 양쪽 변형: 여러 샘플에 `FooTest.java` + `FooJUnit5Test.java` 쌍으로 존재 (CLAUDE.md의 "Testing conventions" 참고)
