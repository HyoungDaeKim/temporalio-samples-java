# Temporal Java SDK: Client/Worker 구성 흐름 — 색인

Temporal Java 샘플(`core/src/main/java/io/temporal/samples/hello/HelloActivity.java`)에서 반복되는 셋업 흐름과 각종 Options를 주제별로 정리했다. 아래 토픽 파일로 분리되어 있다.

---

## 전체 흐름 한눈에 보기

```mermaid
flowchart TD
    config["ClientConfigProfile.load()<br/>환경변수·설정파일 로드"]
    stubs["WorkflowServiceStubs<br/><i>gRPC 커넥션 레이어</i>"]
    client["WorkflowClient<br/><i>워크플로 시작·시그널·쿼리 API</i>"]
    newStub["newWorkflowStub(...)<br/><i>클라이언트 측 워크플로 호출</i>"]
    factory["WorkerFactory<br/><i>워커 공용 설정</i>"]
    worker["Worker<br/><i>TaskQueue별 폴러·동시성</i>"]
    regWf["registerWorkflowImplementationTypes(...)"]
    regAct["registerActivitiesImplementations(...)"]
    start(["factory.start()<br/>워커가 TaskQueue 폴링 시작"])

    stubsOpt(["WorkflowServiceStubsOptions"])
    clientOpt(["WorkflowClientOptions"])
    factoryOpt(["WorkerFactoryOptions"])
    workerOpt(["WorkerOptions"])

    config --> stubs
    stubs --> client
    client -. "클라이언트 측 분기" .-> newStub
    client --> factory
    factory --> worker
    worker --> regWf
    worker --> regAct
    regWf --> start
    regAct --> start

    stubsOpt -. options .-> stubs
    clientOpt -. options .-> client
    factoryOpt -. options .-> factory
    workerOpt -. options .-> worker
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
| 13 | [Nexus 개요](temporal/13-nexus-overview.md) | **서비스 간 호출 프리미티브**의 전체 그림. 3단 주소 체계(Endpoint/Service/Operation), 호출 두 지점(Workflow 코드 vs Worker 등록), ChildWorkflow·일반 호출과의 결정표, 이점·핵심 옵션. 04 §13의 상세 옵션으로 들어가기 전 멘탈 모델. |
| 15 | [Saga 보상 패턴과 Nested Saga](temporal/15-nested-saga.md) | `catch` 안의 `compensate()`가 외부 cancel에 유실되는 함정(detached scope), **자식 워크플로마다 자기 Saga를 가지는 Nested Saga** 패턴(mermaid 다이어그램 + 두 가지 실패 경로 + 설계 원칙 + 체크리스트), `NestedSaga.java` 샘플 레퍼런스. |

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

### SDK 내부와 실행 모델

| # | 파일 | 다루는 내용 |
| --- | --- | --- |
| 11 | [Worker 실행 모델과 Sticky Queue](temporal/11-worker-execution-model.md) | `Workflow.await()` vs `while` 루프, **Stub은 "리모컨"일 뿐 — 실제 workflow 메서드 바디는 Worker가 실행**, 다른 `WORKFLOW_ID`의 독립성, **pull 기반 long-poll task 분산** (Normal+Sticky 두 세트 polling), K8s에서 Pod=Worker 매핑, Sticky Queue 생명주기·fallback·설정값·관찰 지표. |
| 12 | [gRPC Retry (전송 계층 재시도)](temporal/12-grpc-retry.md) | `GrpcRetryer` facade와 Sync/Async 구현, `BackoffThrottler` 공식과 congestion 분기, `GrpcRetryerUtils` 재시도 정책 결정기, `RpcRetryOptions` 기본값, 실제 호출 지점(`GenericWorkflowClientImpl` 등), `GrpcMetricsInterceptor` 자동 계측, 서버 측 `RetryPolicy`와의 비교. **두 겹 메커니즘**(클라이언트 gRPC retry vs 서버 RetryPolicy)의 요약 포함. |
| 14 | [SDK 내부 상태머신 (`WorkflowStateMachines`)](temporal/14-state-machines.md) | WFT 수신 → History Event → Command 변환의 전체 파이프라인(mermaid), `HelloActivity` history로 보는 처리 순서, `WorkflowTaskStateMachine` / `ActivityStateMachine`(12 상태, Cancel 3경합) / `TimerStateMachine`(7 상태) 상태 전이 다이어그램, `ActivityCancellationType` 3종의 통지 시점 매핑, replay 결정론 검증 지점. |

### 참고

| # | 파일 | 다루는 내용 |
| --- | --- | --- |
| 99 | [체크리스트 & 샘플 레퍼런스](temporal/99-checklist.md) | 셋업 완료 전 확인할 체크리스트, 주요 샘플 파일 경로 모음. |

---

## 개념 파악 — Worker 실행 모델과 Sticky Queue

Workflow가 "어디서, 어떤 스레드에서, 어떻게 분산되어" 실행되는지의 멘탈 모델.

### 가장 자주 묻는 질문 — "workflow 메서드는 어디서 실행되나"

```java
GreetingWorkflow workflow = client.newWorkflowStub(GreetingWorkflow.class, opts);
WorkflowClient.start(workflow::getGreeting);   // 이 라인에서 메서드 바디는 실행되지 않음
```

- `workflow` 는 `GreetingWorkflowImpl` 인스턴스가 **아님** — SDK가 만든 **다이내믹 프록시**.
- `start(workflow::getGreeting)` 는 메서드 레퍼런스를 가로채서 **`StartWorkflowExecution` gRPC 요청만 서버로 전송** — 서버 TaskQueue에 workflow task 투입.
- 실제 `getGreeting(...)` **바디는 Worker 프로세스**가 `worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class)` 로 등록해둔 클래스를 리플렉션으로 인스턴스화해서 실행.
- 샘플 레포에선 교육상 Client와 Worker가 같은 `main()`에 공존하지만, 프로덕션에선 **Starter 프로세스 ↔ Worker 프로세스** 가 분리되는 게 일반적. TaskQueue 이름만 맞으면 서버가 중개.

### Task 분산 — pull 기반 long-poll

서버는 Worker 주소를 모름. Worker가 `PollWorkflowTaskQueue` / `PollActivityTaskQueue` gRPC 를 **최대 ~60초 보류 응답**으로 걸어두고, 그동안 task가 들어오면 즉시 반환. 응답 즉시 다음 poll 발사. 그래서 pull 모델인데도 지연은 push 수준.

- **Normal + Sticky 두 세트** polling — 같은 Worker가 공유 TaskQueue와 자기 전용 sticky TaskQueue 양쪽에 각각 poller를 띄움.
- **Poller 수 ≠ 동시 실행 수** — `MaxConcurrentWorkflowTaskPollers` 는 "묻는 스레드 수", `MaxConcurrentWorkflowTaskExecutionSize` 는 "실행 slot". 전자가 적어도 후자가 크면 backpressure.
- **Sticky cache hit** 이면 full replay 생략, miss(새 worker가 받음) 면 전체 history replay 후 sticky 전환.

→ 상세: [11 Worker 실행 모델과 Sticky Queue](temporal/11-worker-execution-model.md). (§4 pull 기반 분산, **§4.5 Long-poll 메커니즘 상세** — Poller vs Executor slot, Workflow/Activity Task 왕복, backpressure, keep-alive, graceful shutdown; §6 Sticky Queue 생명주기·fallback·설정)

---

## 자주 가는 길

- **"Temporal을 처음 본다"** → [00](temporal/00-getting-started.md) → [01](temporal/01-setup.md) → [02](temporal/02-workflow-options.md) → [03](temporal/03-activity.md)
- **"Spring MSA 쓰는데 왜 Temporal?"** → [00의 MSA 매핑 섹션](temporal/00-getting-started.md#1-a-msa-호출과의-매핑--spring-webclientrestclient와-비교)
- **"워크플로가 시작은 됐는데 폴링이 안 된다"** → [99 체크리스트](temporal/99-checklist.md)
- **"Signal이 뭔지 원리부터 알고 싶다"** → [06 메시징](temporal/06-messaging.md)의 Signal 섹션 (`@SignalMethod` 호출 메커니즘 포함)
- **"다른 서비스의 Workflow를 호출하고 싶다"** → [04 조합](temporal/04-composition.md)의 ChildWorkflow + 다른 TaskQueue, 또는 Nexus
- **"Nexus 개념 요약"** → [13 Nexus 개요](temporal/13-nexus-overview.md) (상세는 [04 조합](temporal/04-composition.md)의 §13)
- **"Activity 재시도/타임아웃 조정"** → [03 Activity 옵션](temporal/03-activity.md)
- **"gRPC 호출이 왜 자동 재시도되나 / `UNAVAILABLE` 폭주 진단 / `BackoffThrottler` 공식"** → [12 gRPC Retry](temporal/12-grpc-retry.md)
- **"클라이언트 retry 와 서버 RetryPolicy 가 어떻게 겹치나"** → [12 gRPC Retry의 §15 비교 표](temporal/12-grpc-retry.md)
- **"Workflow Stub과 Worker 차이 / Sticky Queue 원리 / K8s Pod=Worker"** → [11 Worker 실행 모델](temporal/11-worker-execution-model.md)
- **"`WorkflowClient.start(workflow::메서드)` 호출했는데 실제 메서드 바디는 어디서 실행되나"** → [11 §2 Stub은 "리모컨"](temporal/11-worker-execution-model.md) + 위 "개념 파악 — Worker 실행 모델" 섹션
- **"Worker는 서버에서 task를 어떻게 받아오나 / long-poll 기본 메커니즘 / Poller 수 vs Executor slot / backpressure / 네트워크 장비 idle timeout / shutdown과 long-poll CANCELLED"** → [11 §4.5 Long-poll 메커니즘 상세](temporal/11-worker-execution-model.md)
- **"테스트 작성"** → [09 운영과 테스트](temporal/09-ops-and-testing.md)
- **"코드 수정했는데 이미 돌고 있는 워크플로는?"** → [10 버저닝](temporal/10-versioning.md)
- **"Saga 보상이 외부 cancel에 휩쓸려 유실되는 걸 막고 싶다"** → [15 Saga 보상 패턴과 Nested Saga §1](temporal/15-nested-saga.md) (detached scope 패턴)
- **"자식 워크플로마다 자기 Saga가 필요한 경우 / 자식 성공 후 부모 단계가 실패하면 어떻게 되돌리나"** → [15 Saga 보상 패턴과 Nested Saga §2](temporal/15-nested-saga.md) + `core/src/main/java/io/temporal/samples/nestedsaga/NestedSaga.java`
- **"서버 history event 가 Command 로 변환되는 내부 과정 / replay 결정론 검증은 어디서 터지나"** → [14 SDK 내부 상태머신 (`WorkflowStateMachines`)](temporal/14-state-machines.md)
- **"`WorkflowTaskStateMachine` / `ActivityStateMachine` / `TimerStateMachine` 상태 전이 / Cancel 경합 처리"** → [14 SDK 내부 상태머신](temporal/14-state-machines.md)의 각 다이어그램
- **"Timer cancel 하면 서버 확정을 기다리나? / `Workflow.sleep` cancel 시 Promise 는 언제 깨나"** → [14 TimerStateMachine 섹션](temporal/14-state-machines.md#timerstatemachine--가장-단순한-비동기-primitive) (즉시 통지)
