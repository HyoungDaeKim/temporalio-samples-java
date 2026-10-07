# 시간 다루기 — Timer / continue-as-new

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 20. `Timer` / `Workflow.sleep` — 결정적 시간 대기

### 역할
워크플로 안에서 **일정 시간을 기다리는** 유일한 안전한 방법. 둘 다 서버에 Timer를 등록해 fire될 때까지 워크플로를 일시 정지시킨다.

- `Workflow.sleep(Duration)` — **블로킹** 호출. 시간이 지날 때까지 그 자리에서 멈춤
- `Workflow.newTimer(Duration)` — **Promise 반환**. 다른 Promise와 레이싱하거나 `thenApply`로 콜백 연결

> ⚠️ **`Thread.sleep`, `TimeUnit.sleep`, `System.currentTimeMillis` 사용 금지.** 결정성이 깨져 리플레이 실패 또는 `NonDeterministicException`. 반드시 `Workflow.sleep` / `Workflow.currentTimeMillis`.

### 서버 Timer가 어떻게 동작하나

클라이언트처럼 OS 스레드를 블록하는 게 아니다 — 정확히는:

1. 워크플로가 `Workflow.sleep(Duration.ofDays(30))` 호출.
2. SDK가 Workflow Task를 완료시키며 서버에 **`StartTimer` 커맨드**를 보낸다 (duration 포함).
3. 서버가 Timer를 큐에 저장하고 **워커는 메모리에서 워크플로를 치워버린다** (스티키 캐시에서 evict 가능). 비용 0.
4. 30일 뒤 서버가 Timer를 fire → `TimerFired` 이벤트 → 새 Workflow Task 생성.
5. 아무 워커가 집어 들어 리플레이 후 `sleep` 다음 줄부터 계속.

→ **몇 초든 몇 년이든 워커 자원을 거의 안 쓴다.** `SleepForDaysImpl.java`가 30일 Timer로 돌아가는 이유.

### `Workflow.sleep(Duration)` — 가장 단순 (`core/.../terminateworkflow/MyWorkflowImpl.java`, `.../hello/HelloQuery.java`)

```java
Workflow.sleep(Duration.ofSeconds(20));          // 20초 멈춤
Workflow.sleep(100);                              // ms 단위 overload
```

- 반환 전까지 **블로킹**. 이 지점에서 Workflow Task는 완료되고, Timer fire 후 재개.
- `CanceledFailure` throw될 수 있음 — scope cancel이나 외부 cancel 시.

### `Workflow.newTimer(Duration)` — Promise 기반 (`core/.../sleepfordays/SleepForDaysImpl.java`, `.../hello/HelloCancellationScopeWithTimer.java`)

```java
Promise<Void> timer = Workflow.newTimer(Duration.ofDays(30));

// 패턴 A — Signal과 타이머 레이싱 ("30일 지나거나 complete signal 오면")
Workflow.await(() -> timer.isCompleted() || this.complete);

// 패턴 B — Timer를 콜백으로 사용
Workflow.newTimer(Duration.ofSeconds(3))
    .thenApply(ignore -> { scope.cancel(); return null; });

// 패턴 C — Timer + Activity Promise 레이싱
Promise<String> result = Async.function(activities::doIt, input);
Promise.anyOf(result, Workflow.newTimer(Duration.ofSeconds(10))).get();
```

### `TimerOptions` 심화 — 현재는 UI 라벨 전용

Temporal Java SDK의 `TimerOptions`는 **의도적으로 미니멀**하다. 공개된 유일한 옵션이 **`setSummary(String)`** (1.24+) — 서버 Timer 동작 자체는 **duration만**(앞서 설명한 서버 Timer 흐름)으로 완전히 결정되기 때문에, 추가 설정이 거의 필요 없다.

#### 전체 사용 (`core/.../hello/HelloWorkflowTimer.java`)

```java
Promise<Void> timer = Workflow.newTimer(
    Duration.ofSeconds(TIME_SECS),
    TimerOptions.newBuilder()
        .setSummary("Workflow Timer")           // ← 이 1개가 사실상 전부
        .build());
```

#### `setSummary(String)` — 유일한 옵션

| 항목 | 내용 |
| --- | --- |
| 버전 | **1.24+** |
| 영향 | **UI/CLI 라벨만**. Timer fire 시점·동작엔 영향 없음 |
| 저장 | 히스토리의 `TimerStarted` 이벤트에 metadata로 적재 |
| 변경 | 불가 (Timer 생성 시 고정) |
| 길이 가이드 | 짧은 한 줄 — 타임라인의 좁은 노드 칸에 표시됨 |
| 결정성 | 리플레이 안전 — 히스토리에 적재되므로 재주입 |

#### 왜 Summary가 중요한가 — 라벨링의 운영 가치

Workflow 안에 Timer가 하나뿐이면 생략해도 되지만, **여러 Timer가 섞이면 UI 타임라인에서 전부 "Timer"로 보여** 구분이 불가능. Summary는 운영 디버깅의 비용 대비 효과가 크다:

```java
// 여러 Timer가 섞인 Workflow — Summary 없이는 구분 불가
Workflow.newTimer(Duration.ofSeconds(30),
    TimerOptions.newBuilder().setSummary("Reminder email after 30s").build());

Workflow.newTimer(Duration.ofMinutes(5),
    TimerOptions.newBuilder().setSummary("Escalation after 5m").build());

Workflow.newTimer(Duration.ofHours(1),
    TimerOptions.newBuilder().setSummary("Session timeout after 1h").build());
```
→ UI 타임라인에서 세 Timer가 각각 다른 라벨로 보여 "지금 어느 Timer가 fire됐나" 즉시 식별.

#### 어느 호출이 `TimerOptions`를 받는가

| 호출 | `TimerOptions` 받음? | 비고 |
| --- | --- | --- |
| `Workflow.newTimer(Duration)` | **X** | 기본값 사용 |
| `Workflow.newTimer(Duration, TimerOptions)` | **O** | Summary 지정 가능 |
| `Workflow.sleep(Duration)` | **X** | 내부적으로 Timer 사용하지만 options 지정 불가 |
| `Workflow.await(Duration, Supplier<Boolean>)` | X | 내부 Timer options 접근 안 됨 |

→ **Summary를 붙이려면 반드시 `Workflow.newTimer(Duration, TimerOptions)` 사용**. `sleep`에선 Summary 없음.

#### 다른 "Summary" 옵션과의 통일

Temporal SDK 1.24+는 **여러 호출에 비슷한 Summary 패턴**을 뒀다 — 운영 UX 통일 목표:

| 호출 | API | Summary 전용? |
| --- | --- | --- |
| Workflow | `WorkflowOptions.setStaticSummary` + `setStaticDetails` | 2필드 |
| ChildWorkflow | `ChildWorkflowOptions.setStaticSummary` + `setStaticDetails` | 2필드 |
| Activity | `ActivityOptions.setSummary` | 1필드 |
| **Timer** | **`TimerOptions.setSummary`** | **1필드** |
| Nexus | `NexusOperationOptions.setSummary` | 1필드 |

→ Timer는 Activity와 같은 1필드 체계. §6의 "StaticSummary / StaticDetails 심화" 참고.

#### Summary 작성 가이드

- **동사 + 목적**이 명확하게 — "Reminder email after 30s" > "Timer1"
- **duration 명시** — 숫자가 Summary에 있으면 UI에서 duration과 매칭 쉬움
- **동적 값 포함 OK** — `"Session timeout for user " + userId` 같은 패턴
- **민감 정보 금지** — 히스토리에 평문 저장. PII는 Memo + Codec 또는 외부 참조로

#### 서버 Timer 자체의 "숨은 옵션" — 없다

흔한 질문 — "Timer에 priority / jitter / retry 설정은?":

- **Priority** — Timer 자체엔 없음. 작업 우선순위가 필요하면 TaskQueue를 분리.
- **Jitter** — Timer 내부엔 없음. **Workflow 코드에서 구현** — `Workflow.newRandom().nextInt(1000)` ms 추가.
- **Retry** — Timer는 서버가 보장. "fire 안 됐을 경우"의 재시도 개념 자체가 없음.
- **Cancellation** — `TimerOptions`가 아니라 `Promise.cancel()` 또는 `scope.cancel()`로 처리 (§14).

→ TimerOptions가 미니멀한 건 **의도된 설계**. "Timer는 단순한 시간 트리거로 유지"가 Temporal의 철학.

#### 샘플 레퍼런스 — Summary 활용

- `core/src/main/java/io/temporal/samples/hello/HelloWorkflowTimer.java` — 저장소 내 유일한 `TimerOptions.setSummary` 사례. 전역 Workflow Timer 하나를 "Workflow Timer"로 라벨링.

### `sleep` vs `newTimer` 선택

| 상황 | 선택 |
| --- | --- |
| 그냥 N초 멈추고 다음 줄로 | `Workflow.sleep(d)` |
| Signal이나 외부 이벤트와 레이싱 ("N초 지나거나 X 오면") | `Workflow.newTimer(d)` + `Workflow.await` |
| Activity timeout 커스텀 ("3초 안 끝나면 scope.cancel") | `Workflow.newTimer(d).thenApply(...)` (§14 참고) |
| 여러 Promise 중 "가장 먼저 끝나는 것" | `Promise.anyOf(timer, otherPromise)` |

### 관련 API

| API | 설명 |
| --- | --- |
| `Workflow.currentTimeMillis()` | 결정적 현재 시각 (ms). `System.currentTimeMillis` 대신 사용 |
| `Workflow.getLastCompletionResult()` | cron 체인에서 이전 Run의 결과 조회 |
| `Workflow.await(Duration, Supplier<Boolean>)` | 조건 또는 duration 중 먼저 — 내부적으로 Timer 사용 |

### 자주 하는 실수

1. **`Thread.sleep` 사용** — 가장 흔한 실수. 워커 스레드를 실제로 블록하고, 리플레이 시 즉시 지나가 결정성이 깨진다. **반드시 `Workflow.sleep`**.
2. **`System.currentTimeMillis()`로 시간 계산** — 리플레이 때 다른 값 나옴. `Workflow.currentTimeMillis()` 또는 `Workflow.newTimer`.
3. **매우 짧은 반복 Timer** — `while (true) { Workflow.sleep(1); ... }` 같은 패턴은 히스토리에 Timer 이벤트가 계속 쌓임. 큰 batch는 `continue-as-new`로 히스토리 리셋.
4. **Timer cancel 안 하고 scope만 나감** — Timer는 scope에 묶여 있지 않으면 scope exit 후에도 fire된다. scope 밖에서 만든 Timer는 명시적으로 cancel하거나 scope 안에서 만들어야 함.
5. **`newTimer`의 Promise를 버림** — `Workflow.newTimer(d)`만 호출하고 Promise를 버리면 Timer는 서버에 등록은 되지만 fire됐을 때 아무도 안 기다리므로 의미가 없다. `.get()`, `.thenApply(...)`, 또는 `Promise.anyOf`로 소비해야 함. (단, `thenApply` 콜백만 걸고 Promise는 버려도 OK)

### 샘플 레퍼런스
- `Workflow.sleep` 기본: `core/src/main/java/io/temporal/samples/terminateworkflow/MyWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/hello/HelloQuery.java`
- 30일 Timer + Signal 레이싱: `core/src/main/java/io/temporal/samples/sleepfordays/SleepForDaysImpl.java`
- Timer로 Activity timeout 구현: `core/src/main/java/io/temporal/samples/hello/HelloCancellationScopeWithTimer.java`
- `TimerOptions.setSummary`: `core/src/main/java/io/temporal/samples/hello/HelloWorkflowTimer.java`
- Signal + Timer 조합: `core/src/main/java/io/temporal/samples/hello/HelloSignalWithTimer.java`
- 폴링 루프에서 sleep: `core/src/main/java/io/temporal/samples/polling/periodicsequence/PeriodicPollingChildWorkflowImpl.java`
- Timer 지연 시작: `core/src/main/java/io/temporal/samples/hello/HelloDelayedStart.java`

---
## 21. `continue-as-new` — 히스토리를 리셋하고 새 Run으로 이어가기

### 역할
Workflow가 **동일한 Workflow ID로 새 Run을 시작**하고 현재 Run은 종료시킨다. 핵심 목적은 **히스토리 크기 제한**(50,000 이벤트, 50 MB) 회피. 끝없이 돌아야 하는 워크플로(cron 교체, 이벤트 처리 루프, 배치)는 주기적으로 continue-as-new를 호출해 히스토리를 리셋해야 한다.

### 왜 필요한가

Temporal 서버는 **단일 Workflow Run의 히스토리 크기에 하드 리밋**을 둔다:
- **50,000 이벤트** 또는 **50 MB** 중 먼저 걸리는 것
- 넘기면 서버가 강제로 fail시킴

로컬에서 보기엔 그냥 `while (true) { ... }` 루프가 돌아가는 것처럼 보여도, 매 Activity 호출/Timer/Signal이 히스토리에 2~3개 이벤트씩 쌓인다. 수천 번 반복하면 리밋에 도달. **그래서 긴 수명의 워크플로는 반드시 continue-as-new 전략이 필요**.

### 두 가지 호출 방식

**방식 1 — `Workflow.continueAsNew(args...)` (가장 단순)** (`core/.../safemessagepassing/ClusterManagerWorkflowImpl.java`)

```java
Workflow.continueAsNew(new ClusterManagerInput(Optional.of(state), input.isTestContinueAsNew()));
// 이 줄 이후는 실행되지 않음 (현재 Run 즉시 종료)
```
현재 `@WorkflowMethod`를 **같은 시그니처로** 새 Run에서 호출. 가장 흔한 패턴.

**방식 2 — `Workflow.newContinueAsNewStub(T.class)` (다른 Workflow Type / 다른 Options)** (`core/.../hello/HelloPeriodic.java`, `.../batch/iterator/IteratorBatchWorkflowImpl.java`, `.../batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`)

```java
// 필드로 미리 만들어두기도 함
private final IteratorBatchWorkflow nextRun =
    Workflow.newContinueAsNewStub(IteratorBatchWorkflow.class);

@Override
public int processBatch(int pageSize, int offset) {
    ...
    if (records.isEmpty()) return offset;              // 종료 조건
    return nextRun.processBatch(pageSize, offset + records.size());  // 다음 Run으로
}
```
stub 메서드 호출이 **곧 continue-as-new 실행**. 리턴 타입은 선언만 맞춰두고 실제 리턴은 받지 못한다 (현재 Run이 종료되므로).

### 언제 continue-as-new를 호출할지 — 세 가지 신호

**신호 1 — 서버가 추천할 때** (`core/.../safemessagepassing/ClusterManagerWorkflowImpl.java`)

```java
if (Workflow.getInfo().isContinueAsNewSuggested()) {   // 서버가 "슬슬 할 때"라고 알림
    Workflow.continueAsNew(...);
}
```
서버가 히스토리 크기·이벤트 수를 보고 임계점에 다다르면 `true`를 반환. **운영에선 이 신호를 기준으로 삼는 게 가장 안전**.

**신호 2 — 고정 반복 횟수** (`core/.../hello/HelloPeriodic.java`)

```java
private static final int SINGLE_WORKFLOW_ITERATIONS = 10;

for (int i = 0; i < SINGLE_WORKFLOW_ITERATIONS; i++) { ... }

// N회 돌면 무조건 continue-as-new
GreetingWorkflow next = Workflow.newContinueAsNewStub(GreetingWorkflow.class);
next.greetPeriodically(name);
```
각 iteration의 복잡도를 알면 **대략 몇 번 돌릴 수 있는지 예측**해 고정값으로.

**신호 3 — 명시적 히스토리 길이 체크** (`core/.../safemessagepassing/ClusterManagerWorkflowImpl.java`, 테스트용)

```java
if (maxHistoryLength > 0 && Workflow.getInfo().getHistoryLength() > maxHistoryLength) {
    return true;  // continue-as-new 트리거
}
```
주로 **테스트에서** continue-as-new 로직을 작은 임계값으로 검증할 때 사용. 운영은 신호 1 권장.

### 자연스러운 반복 종료 — `IteratorBatchWorkflowImpl`

반복이 **데이터로 끝나는** 경우엔 continue-as-new가 자동으로 종료 역할도 겸한다:

```java
if (records.isEmpty()) return offset;                     // 더 처리할 데이터 없음 → 워크플로 완료
return nextRun.processBatch(pageSize, offset + records.size());  // 데이터 있음 → 다음 Run
```

### 안전하게 호출하기 — Handler 완료 대기

Signal/Update 핸들러가 돌고 있는 중에 continue-as-new를 호출하면 **핸들러가 중단**된다. `core/.../safemessagepassing/ClusterManagerWorkflowImpl.java`가 그래서 이렇게 처리한다:

```java
// 모든 pending Signal/Update 핸들러가 끝날 때까지 대기한 뒤 continue-as-new
Workflow.await(() -> Workflow.isEveryHandlerFinished());
Workflow.continueAsNew(new ClusterManagerInput(Optional.of(state), ...));
```
`Workflow.isEveryHandlerFinished()`로 안전 지점을 확보하는 패턴.

### 무엇이 유지되고 무엇이 리셋되는가

| 유지 | 리셋 |
| --- | --- |
| **Workflow ID** (같은 ID로 이어짐) | **Run ID** (새 Run은 새 Run ID) |
| **TaskQueue** (기본값, override 가능) | **히스토리** 전체 (새 Run에서 처음부터) |
| 입력으로 넘긴 **state** | **in-memory 변수** (명시적으로 넘기지 않으면 사라짐) |
| **Memo, SearchAttributes**(명시적 override 안 하면 유지) | **진행 중이던 Activity/Timer** (전부 cancel됨, 새 Run이 다시 시작) |
| | **진행 중이던 Child Workflow** (ParentClosePolicy에 따라) |

### `ParentClosePolicy`와의 상호작용 (중요)

**continue-as-new는 "부모 종료"로 간주된다** — 부모 Workflow가 Child Workflow를 가지고 있다면, `ChildWorkflowOptions.setParentClosePolicy(...)`에 따라 자식의 운명이 결정된다:

- `TERMINATE` (기본) — continue-as-new 순간 자식도 **같이 종료**됨 (대부분 원하지 않는 결과)
- `ABANDON` — 자식은 **독립적으로 계속** 실행 (fan-out 패턴에서 필수. `core/.../batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`가 바로 이 조합)
- `REQUEST_CANCEL` — 자식에게 cancel 요청

→ **continue-as-new를 쓰는 Workflow가 Child를 띄운다면 `ABANDON` 거의 필수.**

### 자주 하는 실수

1. **state를 넘기지 않음** — in-memory 변수는 새 Run에서 사라진다. `continueAsNew(currentState)`로 **명시적 전달** 필수.
2. **Handler 중간에 호출** — `Workflow.isEveryHandlerFinished()` 대기 없이 호출하면 pending Signal/Update가 유실되고 핸들러가 중단된다.
3. **Child Workflow에 `ParentClosePolicy.TERMINATE`(기본)로 두고 continue-as-new** — 자식들이 전부 죽는다. `ABANDON`으로 설정.
4. **너무 늦게 호출** — 50,000 이벤트에 임박해서 호출하면 그 호출 자체가 실패할 수도. 여유 있게 `isContinueAsNewSuggested()` 기준 사용.
5. **매 iteration마다 호출** — 1회당 히스토리 소비가 적은 작업인데 매번 continue-as-new 호출하면 오버헤드만 커진다. `isContinueAsNewSuggested()` 또는 적정 반복 횟수 기준.
6. **continue-as-new 뒤에 코드를 더 작성** — `Workflow.continueAsNew(...)` 호출 이후 라인은 **실행되지 않는다**. 리턴 전에 호출하고 즉시 리턴.

### 디버깅 — Run 체인 추적

UI에서 Workflow ID로 검색하면 **continue-as-new로 이어진 모든 Run이 체인**으로 보인다 (각 Run의 "First Execution Run ID"가 체인 루트). CLI는 `temporal workflow show --workflow-id <id>`가 전체 체인 표시.

### 샘플 레퍼런스
- 가장 단순 (`Workflow.continueAsNew`): `core/src/main/java/io/temporal/samples/safemessagepassing/ClusterManagerWorkflowImpl.java`
- stub + 반복 횟수 기준: `core/src/main/java/io/temporal/samples/hello/HelloPeriodic.java`
- 자연스러운 종료 + batch iterator: `core/src/main/java/io/temporal/samples/batch/iterator/IteratorBatchWorkflowImpl.java`
- continue-as-new + Child(ABANDON) fan-out: `core/src/main/java/io/temporal/samples/batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`
- Signal + Timer + continue-as-new: `core/src/main/java/io/temporal/samples/hello/HelloSignalWithTimer.java`, `core/src/main/java/io/temporal/samples/hello/HelloAccumulator.java`
- 폴링 루프에서 continue-as-new: `core/src/main/java/io/temporal/samples/polling/periodicsequence/PeriodicPollingChildWorkflowImpl.java`
