# Temporal Java SDK: Client/Worker 구성 흐름 — 색인

Temporal Java 샘플(`core/src/main/java/io/temporal/samples/hello/HelloActivity.java`)에서 반복되는 셋업 흐름과 각종 Options를 주제별로 정리했다. 아래 토픽 파일로 분리되어 있다.

---

## 전체 흐름 한눈에 보기

```
ClientConfigProfile.load()                     # 환경변수/설정파일 로드
          │
          ▼
WorkflowServiceStubs  ◀── WorkflowServiceStubsOptions   (gRPC 커넥션 레이어)
          │
          ▼
WorkflowClient        ◀── WorkflowClientOptions         (워크플로 시작/시그널/쿼리 API)
          │
          ├─────────────► newWorkflowStub(...)   # 클라이언트 측: 워크플로 호출
          │
          ▼
WorkerFactory         ◀── WorkerFactoryOptions          (워커 공용 설정)
          │
          ▼
Worker                ◀── WorkerOptions                 (TaskQueue별 폴러/동시성)
          │
          ├── registerWorkflowImplementationTypes(...)
          └── registerActivitiesImplementations(...)
          │
          ▼
factory.start()                                        # 워커가 TaskQueue 폴링 시작
```

4개 구성요소는 **아래로 갈수록 범위가 좁아진다**: Stubs(물리 커넥션) → Client(네임스페이스/직렬화) → WorkerFactory(JVM 내 모든 워커 공통) → Worker(한 TaskQueue 단위).

---

## 토픽별 가이드

### 입문 — 처음 보는 사람 순서

| # | 파일 | 다루는 내용 |
| --- | --- | --- |
| 00 | [시작하기](temporal/00-getting-started.md) | Temporal Server 설치/실행, 전체 흐름 개요, **Spring MSA 호출과의 매핑** (WebClient/RestClient와 비교). "왜 Temporal이 pod 간 통신을 대체하는가"의 멘탈 모델부터 잡고 싶다면 여기부터. |
| 01 | [Client/Worker 셋업](temporal/01-setup.md) | `WorkflowServiceStubs` (gRPC), `WorkflowClient` (워크플로 조작 API), `WorkerFactory`, `Worker` — 4단계 셋업과 각 Options. |

### Options — 호출 당 설정

| # | 파일 | 다루는 내용 |
| --- | --- | --- |
| 02 | [WorkflowOptions](temporal/02-workflow-options.md) | 워크플로 "한 번의 실행" 정의. `WorkflowIdReusePolicy` 4가지, `SearchAttributes`, `Memo`, `StaticSummary`/`StaticDetails` 심화. |
| 03 | [Activity 옵션](temporal/03-activity.md) | `ActivityOptions`의 4가지 타임아웃, `ActivityCancellationType` 3종, `RetryOptions` 지수 백오프, 워커에 Workflow 등록할 때의 `WorkflowImplementationOptions`, Local Activity 전용 `LocalActivityOptions`. |

### 조합과 동시성

| # | 파일 | 다루는 내용 |
| --- | --- | --- |
| 04 | [Workflow 조합](temporal/04-composition.md) | `Saga.Options` 보상 트랜잭션, `ChildWorkflowOptions`(`ParentClosePolicy`·`ChildWorkflowCancellationType` 심화), Nexus 서비스 호출(`NexusServiceOptions`/`NexusOperationOptions`, 크로스 namespace RPC). |
| 05 | [동시성](temporal/05-concurrency.md) | `CancellationScope`로 async 작업 묶음 취소, `Async`로 Workflow 안에서 비동기 호출, `Promise`/`CompletablePromise` 핸들링. |

### 메시징과 시간

| # | 파일 | 다루는 내용 |
| --- | --- | --- |
| 06 | [메시징](temporal/06-messaging.md) | **`Signal`**(비동기 입력, `@SignalMethod` 호출 메커니즘·프록시/gRPC 라우팅 포함), **`Query`**(상태 조회), **`Update`**(Signal+Query 결합, Validator, WaitPolicy). Signal/Query/Update 비교 결정 트리. |
| 07 | [시간 다루기](temporal/07-time.md) | `Workflow.sleep` / `Workflow.newTimer`, Timer 서버 동작, `continue-as-new`로 히스토리 리셋하고 새 Run으로 이어가는 패턴. |

### 고급과 운영

| # | 파일 | 다루는 내용 |
| --- | --- | --- |
| 08 | [고급](temporal/08-advanced.md) | `SideEffect`/`mutableSideEffect` (비결정적 코드 결정적으로), `WorkflowLock` 뮤텍스, `Interceptor` 체인(Client/Worker/Workflow 가로채기), `DataConverter`/`PayloadConverter`/`PayloadCodec` 직렬화·암호화. |
| 09 | [운영과 테스트](temporal/09-ops-and-testing.md) | `Schedule`(Cron 상위 호환 주기 실행), `Replay`로 결정성 검증, `TestWorkflowRule`/`TestWorkflowEnvironment`/`TestWorkflowExtension` 서버 없이 테스트. |
| 10 | [버저닝](temporal/10-versioning.md) | `Workflow.getVersion` 코드 레벨 Patching, Worker Versioning(Deployment)로 서버가 관리하는 배포 버전. |

### 참고

| # | 파일 | 다루는 내용 |
| --- | --- | --- |
| 99 | [체크리스트 & 샘플 레퍼런스](temporal/99-checklist.md) | 셋업 완료 전 확인할 체크리스트, 주요 샘플 파일 경로 모음. |

---

## 개념 파악 — Nexus (서비스 간 호출의 표준 방식)

Temporal의 **cross-namespace / cross-service 호출 프리미티브**. 다른 Temporal 서비스(또는 다른 cluster)의 Workflow/Activity를 **네트워크 안전하게 Workflow처럼** 호출하도록 만든 공식 추상화 — Spring MSA의 WebClient/RestClient에 가장 가까운 "서비스 간 RPC" 역할을 한다.

### 주소 체계 — 3단 식별자

```
Endpoint  →  Service  →  Operation
   │           │             │
   └ 서버에     └ Java         └ Service 안의
     등록된       interface      메서드 하나
     목적지       (@Service)     (@Operation)
     이름
```

Spring MSA로 치면 `Endpoint`가 "서비스 주소", `Service`가 "컨트롤러", `Operation`이 "엔드포인트" 레벨에 해당. **Endpoint 이름만 서버 설정으로 분리**해두면 호출 코드는 환경(dev/stage/prod)에 종속되지 않는다.

### 호출의 두 지점

```java
// 1) Workflow 코드 안 — 호출 시마다
SampleNexusService svc = Workflow.newNexusServiceStub(
    SampleNexusService.class,
    NexusServiceOptions.newBuilder()
        .setOperationOptions(NexusOperationOptions.newBuilder()
            .setScheduleToCloseTimeout(Duration.ofSeconds(10))
            .build())
        .build());
HelloOutput out = svc.hello(input);

// 2) Worker 등록 시 — 환경별 endpoint 주입 (운영/테스트에서 바꿔치기)
worker.registerWorkflowImplementationTypes(
    WorkflowImplementationOptions.newBuilder()
        .setNexusServiceOptions(Map.of(
            "SampleNexusService",
            NexusServiceOptions.newBuilder().setEndpoint("prod-nexus").build()))
        .build(),
    HelloCallerWorkflowImpl.class);
```

### 언제 Nexus인가 — 다른 방식과 비교

| 상황 | 추천 |
| --- | --- |
| 같은 namespace / 같은 팀의 서브 플로우 분해 | **ChildWorkflow** (같은 코드 베이스, 가벼움) |
| 같은 서비스 안에서 Activity만 다른 Worker 풀에 분산 | **Activity + TaskQueue 분리** |
| **다른 namespace / 다른 팀의 서비스를 Workflow에서 호출** | **Nexus** ← 여기 |
| 외부 애플리케이션이 Workflow를 **시작**만 하면 됨 (서비스 간 호출 아님) | `WorkflowClient.start(...)` (일반 Starter) |

### Nexus의 이점 (ChildWorkflow·일반 호출 대비)

1. **서비스 경계가 명확** — 호출 측은 provider의 코드/jar를 몰라도 됨. interface만 공유하면 끝. REST 스펙 공유와 같은 느낌.
2. **환경별 endpoint 교체가 운영 영역** — 호출 코드 수정 없이 테스트에선 `test-endpoint`, 운영에선 `prod-endpoint`로 swap (`core/.../nexus/caller/CallerWorkflowTest.java`).
3. **cross-cluster 지원** — 다른 Temporal cluster로도 호출 가능. 글로벌 서비스 메시 성격.
4. **Workflow 안에서 호출** — 재시도/타임아웃/취소 전파가 Workflow 결정성 모델 안에서 그대로 동작. Signal/Query/Update와 궁합이 좋음.

### 핵심 옵션

- **`NexusServiceOptions`** — 서비스 전체 수준 (`setEndpoint`, 공통 `setOperationOptions`, operation별 `setOperationMethodOptions`)
- **`NexusOperationOptions`** — operation 호출 수준 (`setScheduleToCloseTimeout`, `setCancellationType`)
- **`NexusOperationCancellationType`** — 4가지 (ABANDON / TRY_CANCEL / WAIT_REQUESTED / WAIT_COMPLETED). 상세는 [04 조합](temporal/04-composition.md#nexusoperationcancellationtype-심화--4가지-동작의-상세-차이) 참고.

### 깊이 파고들기

- [04 조합: §13 Nexus 섹션](temporal/04-composition.md) — 옵션 전체, Cancellation 4가지 심화, ChildWorkflow와의 상세 비교
- [00 시작하기: MSA 매핑](temporal/00-getting-started.md#1-a-msa-호출과의-매핑--spring-webclientrestclient와-비교) — Spring 호출 모델과 Temporal 호출 모델의 전반적 매핑
- 샘플: `core/src/main/java/io/temporal/samples/nexuscancellation/`, `core/src/main/java/io/temporal/samples/nexusmultipleargs/`, `core/src/main/java/io/temporal/samples/nexusmessaging/`

---

## 자주 가는 길

- **"Temporal을 처음 본다"** → [00](temporal/00-getting-started.md) → [01](temporal/01-setup.md) → [02](temporal/02-workflow-options.md) → [03](temporal/03-activity.md)
- **"Spring MSA 쓰는데 왜 Temporal?"** → [00의 MSA 매핑 섹션](temporal/00-getting-started.md#1-a-msa-호출과의-매핑--spring-webclientrestclient와-비교)
- **"워크플로가 시작은 됐는데 폴링이 안 된다"** → [99 체크리스트](temporal/99-checklist.md)
- **"Signal이 뭔지 원리부터 알고 싶다"** → [06 메시징](temporal/06-messaging.md)의 Signal 섹션 (`@SignalMethod` 호출 메커니즘 포함)
- **"다른 서비스의 Workflow를 호출하고 싶다"** → [04 조합](temporal/04-composition.md)의 ChildWorkflow + 다른 TaskQueue, 또는 Nexus
- **"Nexus 개념 요약"** → 위 "개념 파악 — Nexus" 섹션 (상세는 [04 조합](temporal/04-composition.md)의 §13)
- **"Activity 재시도/타임아웃 조정"** → [03 Activity 옵션](temporal/03-activity.md)
- **"테스트 작성"** → [09 운영과 테스트](temporal/09-ops-and-testing.md)
- **"코드 수정했는데 이미 돌고 있는 워크플로는?"** → [10 버저닝](temporal/10-versioning.md)
