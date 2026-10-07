# 메시징 — Signal / Query / Update

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 17. `Signal` — 실행 중인 워크플로에 비동기 입력 보내기

### 역할
**실행 중인 Workflow Execution의 상태를 바깥에서 바꾸는 fire-and-forget 메시지**. 외부 이벤트(사용자 입력, Kafka 메시지, 스케줄 변경 등)를 워크플로 흐름에 주입할 때 사용. 서버가 Signal을 받으면 다음 Workflow Task 때 핸들러 메서드가 실행된다.

### 정의 — Workflow 쪽

```java
// core/.../hello/HelloSignal.java
@WorkflowInterface
public interface GreetingWorkflow {
    @WorkflowMethod List<String> getGreetings();

    @SignalMethod void waitForName(String name);   // void 리턴 필수
    @SignalMethod void exit();                     // 여러 Signal 메서드 가능
}

public static class Impl implements GreetingWorkflow {
    List<String> messageQueue = new ArrayList<>();
    boolean exit = false;

    @Override public List<String> getGreetings() {
        while (true) {
            // Signal 핸들러가 상태를 바꾸면 await 조건 재평가되어 깨어남
            Workflow.await(() -> !messageQueue.isEmpty() || exit);
            if (messageQueue.isEmpty() && exit) return received;
            received.add(messageQueue.remove(0));
        }
    }

    @Override public void waitForName(String name) { messageQueue.add("Hello " + name + "!"); }
    @Override public void exit() { this.exit = true; }
}
```

### 전송 — 클라이언트 쪽

```java
// 1) 이미 실행 중인 워크플로에 Signal (ID만 알면 됨)
GreetingWorkflow stub = client.newWorkflowStub(GreetingWorkflow.class, WORKFLOW_ID);
stub.waitForName("World");                        // 그냥 메서드 호출처럼 보이지만 Signal RPC

// 2) signalWithStart — 없으면 시작, 있으면 Signal
WorkflowStub untyped = WorkflowStub.fromTyped(stub);
untyped.signalWithStart("setLanguage",
    new Object[] {"Spanish"},                      // Signal 인자
    new Object[] {"John"});                        // Workflow 시작 인자
// → core/.../tracing/TracingTest.java
```

### 호출 메커니즘 — 왜 `@SignalMethod` 애너테이션이 필요한가

impl 쪽을 보면 `public void exit() { this.exit = true; }`처럼 평범한 Java 메서드다. "그냥 메서드 호출로도 동작할 것 같은데 왜 애너테이션이 필요하지?"라는 의문이 자연스럽다. 답: **클라이언트는 impl 인스턴스를 호출하는 게 아니다.**

```
[Client JVM]                         [Temporal Server]              [Worker JVM]
workflowById.exit()                                                 (워크플로 실행 중)
    │
    │  workflowById는 impl이 아니라
    │  client.newWorkflowStub(...)이 리턴한
    │  JDK Dynamic Proxy (GreetingWorkflow 인터페이스)
    │
    │  프록시가 호출을 가로채 인터페이스의
    │  @SignalMethod 애너테이션을 보고
    │  "Signal RPC로 보내라"고 판단
    ▼
SignalWorkflowExecution gRPC ──────► 히스토리에 WorkflowExecutionSignaled 이벤트 적재
                                              │
                                              │ TaskQueue에 WorkflowTask 추가
                                              ▼
                                     ◀─────── Worker가 Long-poll로 수신
                                                                    │
                                                                    │ 리플레이 중 Signal 이벤트에 도달
                                                                    │ → GreetingWorkflowImpl.exit() 실행
                                                                    │   (workflow 컨텍스트 안에서)
                                                                    │   → this.exit = true
                                                                    │
                                                                    │ Workflow.await(() -> ... || exit)
                                                                    │ 조건 재평가 → true → 깨어나서 return
```

즉 `workflowById.exit()`은 네트워크를 왕복해 **서버가 보관한 워크플로 상태 위에서** impl의 `exit()`을 실행시킨다. 겉으로는 메서드 호출이지만 바이트코드로는 `프록시 → gRPC → 서버 → Worker → impl` 경로를 탄다.

**애너테이션의 역할**: 프록시가 어떤 종류의 RPC로 변환할지 결정하는 **라우팅 지시자**다. `@WorkflowInterface` 안의 모든 메서드는 `@WorkflowMethod` / `@SignalMethod` / `@QueryMethod` / `@UpdateMethod` 중 하나가 **반드시 있어야** 하며, 없으면 스텁 생성 또는 워크플로 등록 시점에 `IllegalArgumentException`이 터진다. `@SignalMethod`를 `@QueryMethod`로 바꾸면 "void 반환은 Query 불가"라고 또 다른 예외를 던진다.

**impl에 직접 접근해서 `impl.exit = true`로 바꾸면 안 되는 이유**:

1. 클라이언트 JVM엔 impl 참조가 없다. impl 객체는 Worker JVM 안에서 **워크플로 태스크마다 생성·소멸**된다.
2. 설령 접근할 수 있어도 워크플로 컨텍스트 바깥 스레드가 상태를 바꾸면 **결정성(determinism) 규칙 위반** → 리플레이 재현 불가 → 워크플로가 깨진다.
3. Signal은 **히스토리에 영구 기록**되어 Worker 재시작·장애 복구 시 재생된다. 단순 변수 변경으론 얻을 수 없는 보장이다.

### 규칙

| 항목 | 내용 |
| --- | --- |
| 리턴 타입 | `void`만 가능. 결과를 주고 받으려면 `Update` (§다음 Update 섹션 참고) 또는 `CompletablePromise` bridge 사용 |
| 전달 보장 | **At-least-once + ordered** (같은 클라이언트 호출 순서로 서버에 도달). 핸들러는 멱등하게 작성 |
| 블로킹 | 핸들러는 블로킹 금지 (`Workflow.await`로 메인 흐름에서 처리) |
| Activity 호출 | 핸들러에서 Activity 호출 가능하지만 짧게 끝내는 게 좋음. 긴 작업은 메인 흐름으로 넘김 |
| 유실 조건 | 워크플로가 종료된 뒤 보낸 Signal은 **드랍**. `signalWithStart`가 그래서 필요 |
| 리플레이 | 결정적 — 받은 Signal은 히스토리에 `WorkflowExecutionSignaled` 이벤트로 적재되어 리플레이 시 재주입 |

### 핸들러의 전형 패턴

**패턴 1 — 상태 변경 + `Workflow.await`로 메인 흐름 깨우기** (가장 흔함, `HelloSignal.java`)
```java
@SignalMethod public void addItem(Item i) { queue.add(i); }
// 메인에서: Workflow.await(() -> !queue.isEmpty());
```

**패턴 2 — scope.cancel로 작업 중단** (`core/.../hello/HelloUpdateAndCancellationTest.java`)
```java
@SignalMethod public void cancelIt() { scope.cancel(); }
```

**패턴 3 — CompletablePromise로 결과 전달** (§16 Promise 참고, `bookingsyncsaga`)
```java
@SignalMethod public void setData(Data d) { dataPromise.complete(d); }
```

### `signalWithStart` 유즈케이스

- "주문이 들어왔다"는 Signal을 쐈을 때 **처음이면 Workflow 시작, 두 번째부터는 기존 Workflow에 Signal 추가**하는 패턴 (아래 §19 Accumulator 샘플).
- 외부 시스템이 "먼저 start → 그 다음 signal" 두 단계를 하는 대신 **원자적**으로 해결.
- `core/.../hello/HelloSignalWithStartAndWorkflowInit.java` 샘플이 `@WorkflowInit`과 함께 사용하는 고급 패턴.

### 관련 Workflow API

| API | 설명 |
| --- | --- |
| `Workflow.await(Supplier<Boolean>)` | 조건이 true가 될 때까지 블록. Signal이 상태를 바꾸면 자동으로 재평가 |
| `Workflow.await(Duration, Supplier<Boolean>)` | 타임아웃 포함 |
| `WorkflowInbound.signal(...)` | 인터셉터에서 Signal 전처리/후처리 (`core/.../retryonsignalinterceptor/*`) |

### 샘플 레퍼런스
- 기본 Signal + exit 패턴: `core/src/main/java/io/temporal/samples/hello/HelloSignal.java`
- Signal + Timer: `core/src/main/java/io/temporal/samples/hello/HelloSignalWithTimer.java`
- `signalWithStart` + `@WorkflowInit`: `core/src/main/java/io/temporal/samples/hello/HelloSignalWithStartAndWorkflowInit.java`
- 메시지 누적(Accumulator): `core/src/main/java/io/temporal/samples/hello/HelloAccumulator.java`
- 안전한 메시지 전달 패턴: `core/src/main/java/io/temporal/samples/safemessagepassing/ClusterManagerWorkflow.java`
- Signal로 Timer 갱신: `core/src/main/java/io/temporal/samples/updatabletimer/DynamicSleepWorkflow.java`
- Signal + Interceptor로 재시도: `core/src/main/java/io/temporal/samples/retryonsignalinterceptor/RetryOnSignalInterceptorListener.java`

---
## 18. `Query` — 실행 중인 워크플로의 상태를 **읽기**

### 역할
워크플로 상태의 **read-only 스냅샷**을 가져오는 동기 호출. UI나 모니터링이 "지금 진행률이 몇 %인가?", "현재 greeting 값이 뭔가?"처럼 **상태를 조회**할 때 사용. Signal과 달리 **결과가 즉시 반환되고, 히스토리에 적재되지 않는다**.

### 정의 — Workflow 쪽

```java
// core/.../hello/HelloQuery.java
@WorkflowInterface
public interface GreetingWorkflow {
    @WorkflowMethod void createGreeting(String name);

    @QueryMethod String queryGreeting();   // 반환값 필수
}

public static class Impl implements GreetingWorkflow {
    private String greeting;

    @Override public void createGreeting(String name) {
        greeting = "Hello " + name + "!";
        Workflow.sleep(Duration.ofSeconds(2));
        greeting = "Bye " + name + "!";
    }

    @Override public String queryGreeting() { return greeting; }  // 상태 반환
}
```

### 호출 — 클라이언트 쪽

```java
GreetingWorkflow stub = client.newWorkflowStub(GreetingWorkflow.class, WORKFLOW_ID);
WorkflowClient.start(stub::createGreeting, "World");

Thread.sleep(1000);
System.out.println(stub.queryGreeting());  // → "Hello World!"
Thread.sleep(2000);
System.out.println(stub.queryGreeting());  // → "Bye World!"
```
→ 메서드 호출처럼 보이지만 서버로 가는 **QueryWorkflow RPC**.

### 규칙 — Signal과의 차이

| 항목 | **Signal** | **Query** |
| --- | --- | --- |
| 리턴 | `void` | 임의 타입 (반환 필수) |
| 상태 변경 | **가능** | **금지** — 상태 변경 코드는 실행되지 않는 걸 보장해야 함 |
| 히스토리 | `WorkflowExecutionSignaled` 이벤트 적재 | **적재 안 됨** (영향 없음) |
| 리플레이 | 재주입됨 | 매번 **현재 상태로** 실행 |
| 동기/비동기 | 비동기 (fire-and-forget) | **동기** (결과 반환까지 블록) |
| 종료된 워크플로 | 드랍 (`signalWithStart`로 보완) | 완료된 워크플로도 Query 가능 — `QueryRejectCondition`으로 제어 |

> **Query 핸들러에서 절대 상태를 바꾸면 안 된다.** 변경하면 리플레이 때 다른 결과가 나와 결정성이 깨지고 `NonDeterministicException`이 발생할 수 있다. Query는 **순수 함수**.

### 기본 제공 Query — `__stack_trace`, `__query_types`

모든 워크플로에는 자동으로 두 개의 built-in Query가 붙는다.
- `__stack_trace` — 현재 워크플로 코루틴들의 스택 트레이스. 디버깅용 (CLI: `temporal workflow stack`)
- `__query_types` — 등록된 Query 메서드 이름 목록

### `QueryRejectCondition` — 종료 워크플로 Query 제어

클라이언트에서 `WorkflowClientOptions.setQueryRejectCondition(...)`으로 설정:
- `NONE` — 상태 무관 Query 허용 (기본)
- `NOT_OPEN` — RUNNING이 아니면 거절
- `NOT_COMPLETED_CLEANLY` — FAILED/CANCELED/TERMINATED면 거절

### Query vs Update — 선택 가이드

`Update`(1.21+)는 "Signal + Query를 합친 것" — 상태 변경 **과** 결과 반환이 **동기적으로 가능**하고 서버가 중복 제거/검증까지 해준다.

| 상황 | 선택 |
| --- | --- |
| 상태 "읽기만" | **Query** |
| 상태 변경 + 결과 필요 없음 | **Signal** |
| 상태 변경 + 결과 받기 (검증·중복 제거까지) | **Update** (`core/.../hello/HelloUpdate.java`, `springboot/.../update/PurchaseWorkflow.java`) |

### 자주 하는 실수

1. **Query 핸들러에서 Activity 호출** — 금지. Query는 네트워크 호출·블로킹 금지. 미리 저장된 상태만 반환.
2. **Query에서 필드 수정** — 리플레이 비결정성 유발. "읽기만".
3. **Query가 복잡한 계산** — 매 Query마다 실행되므로 비싸질 수 있음. 상태 변경 시 미리 계산해두고 읽기만.
4. **종료된 워크플로 Query 실패에 당황** — `QueryRejectCondition` 또는 `QueryFailedException` 처리 필요.

### 샘플 레퍼런스
- 기본 Query: `core/src/main/java/io/temporal/samples/hello/HelloQuery.java`
- `@QueryMethod`로 상태 조회(`getLastResponse`, `getDocumentCount`): `springai/rag/src/main/java/io/temporal/samples/springai/rag/RagWorkflow.java`
- Query + Signal 조합(진행 상황 추적): `core/src/main/java/io/temporal/samples/packetdelivery/PacketDeliveryWorkflow.java`
- Workflow 종료 후 Query로 결과 조회: `core/src/main/java/io/temporal/samples/listworkflows/CustomerWorkflow.java`
- Query vs Update 비교 예: `core/src/main/java/io/temporal/samples/hello/HelloUpdate.java`, `springboot/src/main/java/io/temporal/samples/springboot/update/PurchaseWorkflow.java`

---
## 19. `Update` — 상태 변경 + 결과 반환을 동기적으로 (Signal + Query의 결합)

### 역할
Signal(상태 변경)과 Query(결과 반환)의 장점을 합친 1.21+ 기능. **서버가 중복 제거·검증·순서를 보장**해주고, 클라이언트는 결과(또는 거절 이유)를 **동기적으로** 받을 수 있다. "주문 접수 → 결과 반환"처럼 실패/성공 피드백이 필요한 interactive 조작에 쓴다.

> 서버 설정 필요 — `frontend.enableUpdateWorkflowExecution=true` (최신 Temporal 서버는 기본 ON).

### 정의 — Workflow 쪽 (`core/.../hello/HelloUpdate.java`)

```java
@WorkflowInterface
public interface GreetingWorkflow {
    @WorkflowMethod List<String> getGreetings();

    @UpdateMethod int addGreeting(String name);   // 리턴값 가능 (Signal과 다름)

    @UpdateValidatorMethod(updateName = "addGreeting")
    void addGreetingValidator(String name);       // 선택적 — 핸들러 실행 전 검증
}

public static class Impl implements GreetingWorkflow {
    private final List<String> messageQueue = new ArrayList<>(10);
    private final List<String> receivedMessages = new ArrayList<>(10);

    @Override
    public int addGreeting(String name) {
        if (name.isEmpty()) {
            // TemporalFailure (ApplicationFailure) → Update가 "실패"로 반환
            // 그 외 예외는 Workflow Task 실패로 처리되어 재시도됨
            throw ApplicationFailure.newFailure("Cannot greet empty name", "Failure");
        }
        messageQueue.add(activities.composeGreeting("Hello", name));  // 상태 변경 OK
        return receivedMessages.size() + messageQueue.size();          // 결과 반환 OK
    }

    @Override
    public void addGreetingValidator(String name) {
        // Validator는 Query와 같은 제약: 상태 변경 금지, 순수 함수
        if (receivedMessages.size() >= 10) {
            // 예외 throw → Update "거절" (히스토리에 적재되지 않음)
            throw new IllegalStateException("Only 10 greetings may be added");
        }
    }
}
```

### 호출 — 클라이언트 쪽

```java
// 1) 동기 호출 (가장 흔함) — 완료까지 블록, 결과 반환
int count = workflow.addGreeting("World");

// 2) 거절/실패는 WorkflowUpdateException으로 올라옴
try {
    workflow.addGreeting("");
} catch (WorkflowUpdateException e) {
    Throwable root = Throwables.getRootCause(e);  // 원래 ApplicationFailure
}

// 3) Untyped stub으로 호출
WorkflowStub untyped = client.newUntypedWorkflowStub(WORKFLOW_ID);
int result = untyped.update("addGreeting", int.class, "Temporal");

// 4) updateWithStart — 없으면 시작하면서 Update, 있으면 Update만
TxResult r = WorkflowClient.executeUpdateWithStart(
    workflow::returnInitResult,
    UpdateOptions.<TxResult>newBuilder().build(),
    new WithStartWorkflowOperation<>(workflow::processTransaction, txRequest));
// → core/.../earlyreturn/EarlyReturnClient.java
```

### 두 단계의 라이프사이클

```
클라이언트가 Update 전송
         │
         ▼
① Validator 실행 (optional)
   - 예외 throw → "Rejected"로 즉시 반환, 히스토리 적재 X
   - 통과 → "Accepted"
         │
         ▼
② Handler 실행
   - 상태 변경, Activity 호출, Promise 대기 가능
   - ApplicationFailure throw → "Failed"로 반환 (히스토리 적재 O)
   - 그 외 예외 → Workflow Task 실패 → 재시도
   - return → "Completed" (결과 클라이언트에 반환)
```

### Validator 규칙 — Query와 같은 제약

- **상태 변경 금지** (필드 수정 X)
- **Activity 호출 금지, 블로킹 금지**
- 거절 사유는 **임의 예외**로 표현. 거절된 Update는 **히스토리에 적재되지 않아** 재시도 대상도 아님.
- Validator 생략 가능 — 핸들러 안에서 바로 `ApplicationFailure` 던져도 됨 (단, 그 경우 히스토리에 적재됨).

### Signal / Query / Update 비교 (상세)

#### 1) 기본 성격

| 항목 | **Signal** | **Query** | **Update** |
| --- | --- | --- | --- |
| 어노테이션 | `@SignalMethod` | `@QueryMethod` | `@UpdateMethod` (+ 선택 `@UpdateValidatorMethod`) |
| 리턴 타입 | **`void` 전용** | 임의 타입 (반환 필수) | 임의 타입 (`void` 포함 가능) |
| 호출 성격 | fire-and-forget | 상태 **읽기** | 상태 변경 + 결과 반환 |
| 도입 시기 | Temporal 1.0부터 | Temporal 1.0부터 | Temporal **1.21+** (서버 플래그 필요) |
| 핸들러 개수 제한 | 다수 허용 | 다수 허용 | 다수 허용 |
| 이름 override | `@SignalMethod(name="...")` | `@QueryMethod(name="...")` | `@UpdateMethod(name="...")` |

#### 2) 호출자(클라이언트) 관점

| 항목 | **Signal** | **Query** | **Update** |
| --- | --- | --- | --- |
| 동기/비동기 | **비동기** — 서버가 ack하면 즉시 반환 | **동기** — 결과까지 블록 | **동기** (기본, `WaitPolicy`로 조절) |
| 결과 수신 | 없음 | 리턴 값 | 리턴 값 또는 `WorkflowUpdateException` |
| "with-start" 변형 | **`signalWithStart`** | X | **`updateWithStart`** (1.26+) |
| 중복 제거 | X (매번 전달) | X (매번 실행) | **O** — 같은 `UpdateID`면 서버가 중복 거르고 기존 결과 반환 |
| 호출자 API (typed) | `workflow.someSignal(args)` | `workflow.someQuery()` | `workflow.someUpdate(args)` |
| 호출자 API (untyped) | `stub.signal("name", args)` | `stub.query("name", type)` | `stub.update("name", type, args)` / `stub.startUpdate(...)` |
| 폴링 / 비동기 핸들 | X | X | **`startUpdate`** → `UpdateHandle.getResultAsync()` |

#### 3) Workflow 내 실행 모델

| 항목 | **Signal** | **Query** | **Update** |
| --- | --- | --- | --- |
| 상태 변경 (필드 수정) | **O** | **X** (금지, 결정성 깨짐) | **O** |
| Activity 호출 | **O** (가능하되 짧게 권장) | **X** | **O** |
| ChildWorkflow / Nexus / Timer | **O** | **X** | **O** |
| `Workflow.await` 블로킹 | **O** | **X** | **O** |
| 조건이 만족될 때까지 대기 | O (`Workflow.await`) | X — **즉시 반환해야 함** | O (`Workflow.await`) |
| Promise 대기 | O | **X** | O |
| `Workflow.sleep` | O | **X** | O |
| 핸들러에서 다른 Signal/Update 호출 가능? | O (내부 상태 조작으로) | X | O |
| 리플레이 시 실행? | **O** (재주입됨) | **X** (매번 현재 상태로 실행) | **O** (재주입됨) |
| Validator 메서드 존재? | X | X | **`@UpdateValidatorMethod`** (선택) |

#### 4) 서버·히스토리 측면

| 항목 | **Signal** | **Query** | **Update** |
| --- | --- | --- | --- |
| 히스토리 적재 | **O** (`WorkflowExecutionSignaled`) | **X** (적재 안 됨) | **O** (`Accepted` / `Completed` / `Failed`. **Rejected는 적재 안 됨**) |
| 서버 스토리지 비용 | 발생 | 없음 | 발생 |
| 리플레이 대상? | O | X | O |
| 종료된 워크플로에 보낼 수 있나? | **X** (드랍, exception) | **O** (완료된 워크플로도 Query 가능) | **X** (실패) |
| 전달 보장 | **At-least-once + ordered** | 매번 1회 실행 | **At-most-once per UpdateID** (중복 제거) |
| 서버 쪽 throttling | 서버 설정에 따름 | 없음 | 서버 설정에 따름 |

#### 5) 실패·거절·에러 모델

| 항목 | **Signal** | **Query** | **Update** |
| --- | --- | --- | --- |
| 거절(reject) 가능? | **X** (워크플로가 무조건 받음) | **X** (실행 자체는 성공) | **O** (Validator에서 임의 예외 throw) |
| Validator 실패 처리 | — | — | **Rejected** — 히스토리 적재 X, 재시도 X |
| 핸들러 중 `ApplicationFailure` throw | 핸들러 다시 실행 안 됨, Workflow Task 성공 | `QueryFailedException`으로 전달 | **Failed** — 히스토리 적재 O, 결과로 전달 |
| 핸들러 중 **그 외 예외** throw | **Workflow Task 실패** → 재시도 → stuck 가능 | `QueryFailedException` | **Workflow Task 실패** → 재시도 |
| 클라이언트 쪽 수신 exception | 없음 (fire-and-forget) | `WorkflowQueryException` | **`WorkflowUpdateException`** (원인 `getCause()`에 Application/Canceled 등) |
| 종료 후 호출 시 | 드랍 (완전 종료 후엔 exception) | **`QueryRejectCondition`**으로 제어 | 실패 |

#### 6) 완료·종료 상호작용

| 항목 | **Signal** | **Query** | **Update** |
| --- | --- | --- | --- |
| Workflow 완료를 **블록**함? | X — 핸들러는 비동기, 메인이 끝나면 종료 | X — 즉시 실행 | **O** — 처리 중인 Update가 있으면 Workflow 완료 지연 (`HelloUpdate.java` 주석 참고) |
| continue-as-new 전에 대기 필요? | **O** (`Workflow.isEveryHandlerFinished()` + `Workflow.await`) | X | **O** (`Workflow.isEveryHandlerFinished()`) |
| Workflow cancel 시 핸들러 중단? | O (detached scope로 보호 가능) | 해당 없음 | O (detached scope로 보호 가능) |

#### 7) 관찰·운영

| 항목 | **Signal** | **Query** | **Update** |
| --- | --- | --- | --- |
| UI/CLI 노출 | `temporal workflow signal` | `temporal workflow query` | `temporal workflow update` |
| 히스토리 이벤트로 추적 | O | X (별도 로깅 필요) | O (단, Rejected는 추적 불가) |
| 디버깅용 built-in 조회 | 없음 | `__stack_trace`, `__query_types` | 없음 |
| 멱등성 책임 | **클라이언트/핸들러 양쪽** | 핸들러만 순수 | **서버 중복 제거** + 핸들러 멱등 책임 보완 |

### 결정 트리 — "뭘 써야 하지?"

```
결과를 받을 필요가 있는가?
├── No → Signal (상태 변경만, 비동기)
└── Yes
    ├── 상태 변경이 전혀 없는 단순 조회인가? → Query
    └── 상태 변경 포함 (혹은 변경할 수도 있음)
        ├── 호출자가 "성공/거절" 피드백 즉시 필요 → Update
        └── 피드백 불필요 + Workflow가 결과를 별도 경로로 저장 → Signal + Query 조합
```

**상황별 패턴**
- **"주문 접수"** — Update (검증·중복 제거·결과 반환 모두 필요)
- **"설정 변경"** (결과 불필요) — Signal
- **"현재 진행률"** — Query
- **"현재 상태 + 변경"** 자주 반복 — Update
- **"외부 Kafka 메시지 주입"** — Signal (fire-and-forget 성격 자연)
- **"관리자가 Workflow를 멈춤"** — Signal + 상태 변경 (`exit=true`) 또는 Workflow cancel
- **"현재 cart 조회 + 아이템 추가 + 추가 성공 여부"** — Update
- **"관리자용 디버깅 조회"** — Query + `__stack_trace`

### `WaitPolicy` — 어느 단계까지 기다릴지

`UpdateOptions.setWaitPolicy(...)`로 클라이언트가 동기 대기 수준을 조절:

| 값 | 반환 시점 |
| --- | --- |
| `ADMITTED` | 서버가 Update 접수만 하면 반환 (가장 빠름, 결과 못 받음) |
| `ACCEPTED` | Validator 통과까지 대기 |
| `COMPLETED` (기본) | Handler 완료까지 대기 — 결과·실패 모두 받음 |

> `startUpdate(...)` + 이후에 `getResult()` 호출로도 비동기 분리 가능. "Validator만 통과 확인하고 Handler는 백그라운드"로 처리할 때 유용.

### `updateWithStart` 유즈케이스 (`core/.../earlyreturn/EarlyReturnClient.java`)

"Workflow 시작 + 첫 Update를 **원자적으로** 실행하고 Update 결과만 **즉시** 받고 싶을 때".
전형 사례: 트랜잭션 접수 워크플로가 길지만, 클라이언트는 **"접수 성공했는지"**(ID 발급 결과)만 즉시 받으면 되고 나머지는 백그라운드.

```java
// Workflow는 계속 돌지만 updateWithStart는 Update 결과(TxResult)만 받아 반환
TxResult early = WorkflowClient.executeUpdateWithStart(
    workflow::returnInitResult,                                      // Update
    UpdateOptions.<TxResult>newBuilder().build(),
    new WithStartWorkflowOperation<>(workflow::processTransaction, req));  // Workflow 시작

// 이후 나중에 전체 결과 받고 싶으면
TxResult full = WorkflowStub.fromTyped(workflow).getResult(TxResult.class);
```

### 자주 하는 실수

1. **Validator에서 상태 변경** — Query와 같은 제약. 비결정성 유발, 리플레이 실패.
2. **거절을 일반 `RuntimeException`으로 throw** — Workflow Task 실패로 처리돼 **재시도**된다. 거절은 반드시 `ApplicationFailure.newFailure(...)` 또는 Validator에서 throw.
3. **Handler에서 긴 Activity를 돌리고 동기 대기** — 기본 `WaitPolicy.COMPLETED`로 호출한 클라이언트가 그만큼 블록된다. 긴 작업이면 `ACCEPTED`로 반환하거나 Signal로 전환 고려.
4. **"Update가 끝나면 워크플로도 끝나겠지" 가정** — Update 완료는 워크플로 완료와 무관. 심지어 **Workflow가 완료되려 해도 pending Update가 있으면 완료를 미룬다** (`HelloUpdate.java` 주석 참고).
5. **멱등성 가정** — Update는 서버가 UpdateID로 중복 제거하지만, 같은 Update를 다른 UpdateID로 보내면 두 번 실행된다. 비즈니스 멱등성은 여전히 핸들러가 책임.

### 샘플 레퍼런스
- 기본 Update + Validator + Rejected/Failed 처리: `core/src/main/java/io/temporal/samples/hello/HelloUpdate.java`
- Update + CancellationScope 조합: `core/src/test/java/io/temporal/samples/hello/HelloUpdateAndCancellationTest.java`
- `updateWithStart` 조기 반환 패턴: `core/src/main/java/io/temporal/samples/earlyreturn/TransactionWorkflow.java`, `core/src/main/java/io/temporal/samples/earlyreturn/EarlyReturnClient.java`
- Spring Boot Update + Validator: `springboot/src/main/java/io/temporal/samples/springboot/update/PurchaseWorkflow.java`
- Chat 턴을 Update로(결과 반환): `springai/basic/src/main/java/io/temporal/samples/springai/chat/ChatWorkflow.java`
- CompletablePromise bridge로 Update ↔ 메인 흐름 연결: `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflow.java`
- 안전한 메시지 전달에서 Update 사용: `core/src/main/java/io/temporal/samples/safemessagepassing/ClusterManagerWorkflow.java`
