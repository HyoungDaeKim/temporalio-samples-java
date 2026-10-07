# 고급 — SideEffect / Lock / Interceptor / DataConverter

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 22. `SideEffect` / `mutableSideEffect` — 비결정적 코드를 결정적으로

### 역할
Workflow 코드는 **결정적**이어야 리플레이가 안전하다 — `new Random()`, `UUID.randomUUID()`, `System.currentTimeMillis()`, `System.getenv(...)` 같은 호출은 매번 다른 값을 내므로 리플레이 시 다른 경로로 흘러 비결정성 예외를 유발한다. `Workflow.sideEffect`는 **그 호출을 1번만 실행하고 결과를 히스토리에 marker로 적재**해, 리플레이 때는 marker에서 값을 돌려줘 결정성을 보장한다.

### 이미 결정적인 Workflow API (대부분의 경우 이걸 먼저 고려)

sideEffect를 쓰기 전에, Temporal이 이미 결정적 대체 API를 제공하는지 확인:

| 비결정적 (쓰면 안 됨) | 결정적 대체 |
| --- | --- |
| `new Random()` | `Workflow.newRandom()` |
| `UUID.randomUUID()` | `Workflow.randomUUID()` |
| `System.currentTimeMillis()` | `Workflow.currentTimeMillis()` |
| `LoggerFactory.getLogger(cls)` | `Workflow.getLogger(cls)` (리플레이 로그 억제) |
| `Thread.sleep` | `Workflow.sleep` (§20) |

→ **대체 API가 있으면 sideEffect 쓸 필요 없음.** sideEffect는 "SDK가 제공하지 않는 결정 불가능한 로컬 계산"에만.

### `Workflow.sideEffect(Class, Supplier)` (`core/.../hello/HelloSideEffect.java`)

```java
int sideEffectsRandomInt = Workflow.sideEffect(
    int.class,
    () -> {
        Random random = new SecureRandom();   // Workflow.newRandom으로 안 되는 경우
        return random.nextInt();
    });
```

- 첫 실행 시: 람다 실행 → 결과를 `MarkerRecorded` 이벤트로 히스토리에 적재.
- 리플레이 시: 람다 실행 없이 marker에서 값 반환.
- 리턴 타입은 **직렬화 가능**해야 함 (`DataConverter`로 처리).

> **Activity와의 차이** — Activity는 "원격·오래 걸리는·실패 가능한 작업"용. sideEffect는 "로컬·즉시·실패 안 하는 짧은 계산"용. 네트워크 호출은 **반드시 Activity**로.

### `Workflow.mutableSideEffect(id, Class, Comparator, Supplier)` — 값이 바뀔 수 있는 경우

같은 id로 여러 번 호출될 수 있고, **값이 변경될 때만 새 marker를 적재**. 리플레이 때는 **마지막 marker 값**을 반환.

```java
int version = Workflow.mutableSideEffect(
    "config-version",
    Integer.class,
    (a, b) -> a.equals(b),                     // "같다"의 정의
    () -> loadConfigVersionFromMemory());
```
→ 외부 상태(config, feature flag)를 Workflow에 반영하되 **값이 바뀔 때만** 히스토리에 기록. 용량 절약.

### Spring AI 샘플 — SideEffectTool 패턴 (`springai/basic/.../TimestampTools.java`, `ChatWorkflowImpl.java`)

LLM이 호출하는 "툴" 중 결정적이지 않은 것(현재 시각 조회 등)을 `@SideEffectTool`로 선언하면 자동으로 `Workflow.sideEffect`로 감싼다. "Activity까지는 과한데 비결정적인 짧은 계산"에 잘 맞는 예.

### `Workflow.newRandom()` vs `Workflow.sideEffect(() -> new Random())` — 어느 쪽?

`Workflow.newRandom()`은 **seed를 Workflow Run ID로 고정**해 같은 Run은 리플레이 때도 같은 수열을 낸다. 외부 Marker 없이 결정성 보장. 대부분 이쪽이 더 효율적.

`sideEffect(() -> new SecureRandom().nextInt())`는 매 호출마다 marker를 적재하므로 **진짜 난수**(암호학적)가 필요할 때. 비용 더 큼.

### 자주 하는 실수

1. **sideEffect 안에서 Workflow API 호출** — `Workflow.sleep`, `Workflow.await`, Activity 호출, Signal 핸들링 등 **전부 금지**. sideEffect 람다는 **순수 로컬 계산만**. 안에서 `Workflow.sleep` 호출하면 `IllegalStateException`.
2. **sideEffect 안에서 I/O (파일/네트워크)** — 결정성 깨짐 + 실패 가능. 그런 작업은 Activity로.
3. **매우 큰 결과 반환** — 히스토리에 적재되므로 크기 주의. 큰 데이터는 Activity + external storage pattern.
4. **mutableSideEffect를 Workflow.sideEffect처럼 쓰기** — id가 같으면 값이 바뀔 때만 marker 추가. 매번 적재가 필요하면 `sideEffect` 사용.
5. **단순히 `Workflow.newRandom()`으로 충분한데 sideEffect 사용** — 불필요한 marker 적재. 먼저 결정적 API 있는지 확인.

### 샘플 레퍼런스
- 기본 SideEffect + 결정적 대체 API 비교: `core/src/main/java/io/temporal/samples/hello/HelloSideEffect.java`
- Spring AI SideEffectTool 패턴: `springai/basic/src/main/java/io/temporal/samples/springai/chat/TimestampTools.java`, `springai/basic/src/main/java/io/temporal/samples/springai/chat/ChatWorkflowImpl.java`

---
## 23. `WorkflowLock` — 워크플로 내부 뮤텍스

### 역할
한 Workflow Execution 안에서 **동시에 돌 수 있는 핸들러·Async 작업들이 공유 상태를 안전하게 변경**하도록 보장하는 뮤텍스. `java.util.concurrent.locks.Lock`과 비슷한 API지만 **Workflow 결정적 스케줄러에서 동작**한다 (`java.util.concurrent.Lock`을 쓰면 결정성 깨짐).

### 왜 필요한가 — Workflow는 single-threaded 아닌가?

Workflow 코드는 OS 레벨에서는 single-thread지만, **Signal/Update 핸들러나 `Async.function`은 메인 흐름과 코루틴처럼 스케줄링**된다. 즉:

- 메인 흐름이 `Workflow.await`, `Workflow.sleep`, `activity.call()`로 **양보(yield)**하면
- 그 사이에 Signal/Update 핸들러가 끼어들어 **같은 필드를 수정**할 수 있다

→ **"Activity 호출 전에 조건 체크 → Activity 호출 → 결과로 상태 수정" 흐름이 중간에 다른 핸들러에 의해 깨질 수 있다**. 이걸 막는 게 `WorkflowLock`.

### 샘플 — Cluster Manager (`core/.../safemessagepassing/ClusterManagerWorkflowImpl.java`)

여러 Update 핸들러(`assignNodesToJob`, `deleteJob`)와 백그라운드 루프(`performHealthChecks`)가 **같은 `state.nodes` Map을 수정**한다. Activity 호출로 yield되는 동안 interleaving이 발생할 수 있어 전부 하나의 `WorkflowLock`으로 보호:

```java
public class ClusterManagerWorkflowImpl implements ClusterManagerWorkflow {
    private final WorkflowLock nodeLock;

    @WorkflowInit
    public ClusterManagerWorkflowImpl(ClusterManagerInput input) {
        nodeLock = Workflow.newWorkflowLock();   // 생성자에서 초기화
        ...
    }

    @Override
    public ClusterManagerAssignNodesToJobResult assignNodesToJobs(
            ClusterManagerAssignNodesToJobInput input) {
        ...
        nodeLock.lock();                         // ← 획득 (yield 발생 가능 — 다른 핸들러가 lock 쥐고 있으면 대기)
        try {
            if (state.jobAssigned.contains(input.getJobName())) {   // 멱등성 체크
                return new ClusterManagerAssignNodesToJobResult(...);
            }
            Set<String> unassignedNodes = getUnassignedNodes();
            ...
            // ⚠️ Activity 호출 — 여기서 yield되지만 다른 핸들러는 lock 못 잡아 안전
            activities.assignNodesToJob(...);
            for (String node : nodesToAssign) state.nodes.put(node, Optional.of(input.getJobName()));
            state.jobAssigned.add(input.getJobName());
            return new ClusterManagerAssignNodesToJobResult(nodesToAssign);
        } finally {
            nodeLock.unlock();                   // ← 반드시 finally에서
        }
    }
}
```

주석에 명시된 설명: *"This call would be dangerous without nodesLock because it yields control and allows interleaving with deleteJob and performHealthChecks, which both touch this.state.nodes."*

### 생성과 API

```java
WorkflowLock lock = Workflow.newWorkflowLock();
```

| 메서드 | 설명 |
| --- | --- |
| `lock.lock()` | 획득 (다른 흐름이 쥐고 있으면 대기 — Workflow Task 완료 후 재개) |
| `lock.lock(Duration timeout)` | 타임아웃 포함 획득. 반환 `boolean` — 성공 여부 |
| `lock.tryLock()` | 즉시 시도, 못 잡으면 `false` |
| `lock.tryLock(Duration)` | tryLock + 타임아웃 |
| `lock.unlock()` | 해제. 반드시 `finally`에서 |
| `lock.isHeld()` | 누군가 쥐고 있는지 확인 |

**재진입 가능(reentrant)** — 같은 "실행 흐름"이면 중첩 lock 가능.

### Signal vs Update vs Lock — 역할 비교

| 도구 | 다루는 문제 |
| --- | --- |
| **Signal** | 외부 이벤트를 워크플로에 **주입** |
| **Update** | 외부에서 상태 변경 + 결과 받기 (서버 중복 제거·검증) |
| **WorkflowLock** | **내부**에서 여러 핸들러/Async가 공유 상태를 **안전하게 변경** |
| **CancellationScope** | 묶음 작업을 함께 **취소** |

### 자주 하는 실수

1. **`java.util.concurrent.locks.Lock`이나 `synchronized` 사용** — 결정성 깨짐 + 리플레이 실패. 반드시 `Workflow.newWorkflowLock()`.
2. **`unlock()`을 `finally` 안 쓰기** — 예외 시 lock이 영구 미해제 → 다른 핸들러들이 영원히 대기.
3. **Lock 바깥에서 공유 state를 수정** — Lock 안에서 `activity.call()` 전 조건을 체크해도, 다른 핸들러가 Lock 바깥 경로로 state를 바꾸면 무용지물. 모든 수정 경로를 Lock 안으로.
4. **Lock 걸고 긴 Activity 호출** — 그 동안 다른 핸들러들이 전부 대기. 긴 작업이면 Lock 해제 후 호출하거나 설계 재검토.
5. **`continue-as-new` 시 lock 상태 유실** — Lock은 Run 로컬. 새 Run에서는 새 Lock. continue-as-new 전에 `Workflow.isEveryHandlerFinished()` 대기 패턴이 그래서 필요(§21 참고).

### 샘플 레퍼런스
- `Workflow.newWorkflowLock` + 다중 Update 핸들러 보호: `core/src/main/java/io/temporal/samples/safemessagepassing/ClusterManagerWorkflowImpl.java` (저장소 내 유일한 예 — 안전한 메시지 전달 패턴 전체 샘플)

---
## 24. `Interceptor` — Workflow/Activity/Client 호출을 가로채기

### 역할
Workflow·Activity·Client 호출의 **모든 경로를 가로채** 메트릭, 트레이싱, 로깅, 재시도, 커스텀 어노테이션 등을 **공통 코드로** 주입하는 메커니즘. AOP 비슷하지만 Temporal이 결정성·리플레이·서버 왕복까지 전부 고려해 설계한 전용 체인.

### 두 축 — 어디에 거느냐 × 어디를 가로채느냐

| 어디에 거는가 | 설정 지점 | 가로챌 수 있는 범위 |
| --- | --- | --- |
| **클라이언트** | `WorkflowClientOptions.setInterceptors(...)` | 외부 → 서버 호출 (start, signal, query, update 등) |
| **워커** | `WorkerFactoryOptions.setWorkerInterceptors(...)` | 서버 → Workflow/Activity 실행 (inbound/outbound 전부) |

**워커 interceptor 1개가 다음 4개 하위 interceptor를 주입할 수 있음** (`core/.../countinterceptor/SimpleCountWorkerInterceptor.java`):

```java
public class SimpleCountWorkerInterceptor extends WorkerInterceptorBase {
    @Override
    public WorkflowInboundCallsInterceptor interceptWorkflow(WorkflowInboundCallsInterceptor next) {
        return new SimpleCountWorkflowInboundCallsInterceptor(next);  // Workflow 수신 (start, signal, query, update)
    }
    @Override
    public ActivityInboundCallsInterceptor interceptActivity(ActivityInboundCallsInterceptor next) {
        return new SimpleCountActivityInboundCallsInterceptor(next);  // Activity 수신 (execute)
    }
}
```

- **WorkflowInboundCallsInterceptor** — 서버 → Workflow (workflow start, signal/update/query 수신)
- **WorkflowOutboundCallsInterceptor** — Workflow → 서버 (Activity 호출, Timer, ChildWorkflow, Signal 발송 등 Workflow 코드에서 나가는 모든 요청)
- **ActivityInboundCallsInterceptor** — 서버 → Activity (Activity 실행 시작·완료)
- **WorkflowClientInterceptor** — 클라이언트 측 (start/signal/query/update, 서버에 보내는 요청)

### 설정 (`core/.../countinterceptor/InterceptorStarter.java`)

```java
// 클라이언트 쪽
WorkflowClient client = WorkflowClient.newInstance(
    service,
    WorkflowClientOptions.newBuilder()
        .setInterceptors(new SimpleClientInterceptor(clientCounter))
        .build());

// 워커 쪽
WorkerFactoryOptions wfo = WorkerFactoryOptions.newBuilder()
    .setWorkerInterceptors(new SimpleCountWorkerInterceptor(workerCounter))
    .validateAndBuildWithDefaults();
WorkerFactory factory = WorkerFactory.newInstance(client, wfo);
```

### 저장소 샘플 — 가장 흔한 유즈케이스

| 샘플 | 유즈케이스 |
| --- | --- |
| `core/.../countinterceptor/*` | 호출 횟수 카운팅 — 가장 단순한 inbound/outbound 체인 예시 |
| `core/.../tracing/*` | **OpenTracing/Jaeger 분산 트레이싱** — SDK 제공 `OpenTracingClientInterceptor` / `OpenTracingWorkerInterceptor` 사용 |
| `core/.../retryonsignalinterceptor/*` | Signal을 받으면 실패한 Activity를 **재시도**하는 패턴 — outbound interceptor가 Activity 결과를 가로채 Signal 대기로 변환 |
| `core/.../excludefrominterceptor/*` | 특정 Workflow Type을 **interceptor에서 제외** — `getWorkflowType()`으로 분기 |
| `core/.../customannotation/*` | 커스텀 어노테이션(`@BenignExceptionTypes`) 처리 — Workflow 메서드에 어노테이션 붙이면 interceptor가 자동으로 해석 |
| `core/.../nexuscontextpropagation/*` | Nexus 호출에 MDC 컨텍스트 주입 — `WorkflowOutboundCallsInterceptor` + `NexusInboundInterceptor` |

### 작성 패턴 — Base 클래스 상속

SDK가 `*Base` 클래스를 제공한다 — 전부 override할 필요 없이 **필요한 메서드만** 재정의:

```java
public class MyWorkflowInbound extends WorkflowInboundCallsInterceptorBase {
    public MyWorkflowInbound(WorkflowInboundCallsInterceptor next) { super(next); }

    @Override
    public SignalInput signalWorkflow(SignalInput input) {
        log.info("Signal {} received", input.getSignalName());
        return super.signalWorkflow(input);                 // 체인의 다음 interceptor로
    }
}
```

### 리플레이 안전성

**outbound interceptor는 리플레이 시에도 실행된다.** 그래서 `WorkflowOutboundCallsInterceptor`에서는 **비결정적 코드 금지**(§22 SideEffect와 같은 제약). 예: `System.currentTimeMillis()`, `UUID.randomUUID()` 쓰면 안 됨.

Inbound interceptor는 "서버에서 들어오는 입력을 처리"하므로 비결정성 걱정은 덜하지만, Workflow 코드 안에서 호출되는 부분(`execute`, `handleSignal`)은 똑같이 결정적이어야 함.

### `WorkflowImplementationOptions`로 특정 Workflow만 제외 (`core/.../excludefrominterceptor/*`)

```java
public class MyWorkerInterceptor extends WorkerInterceptorBase {
    @Override
    public WorkflowInboundCallsInterceptor interceptWorkflow(WorkflowInboundCallsInterceptor next) {
        return new MyWorkflowInboundCallsInterceptor(next, excludedTypes);
    }
}

// Inbound에서 Workflow type 확인 후 분기
if (excludedTypes.contains(ctx.getWorkflowType())) {
    return super.execute(input);                 // 그냥 넘김
} else {
    // ... 커스텀 로직
}
```

### 자주 하는 실수

1. **`super.XXX(input)` 호출 누락** — 체인이 끊어져 Workflow가 실제로 실행되지 않음. 거의 모든 메서드는 `next` 호출 필수.
2. **outbound interceptor에서 비결정 코드** — 리플레이 때 다른 값 → `NonDeterministicException`.
3. **interceptor 안에서 긴 작업/네트워크 호출** — Workflow Task 처리를 블록. 그런 작업은 Activity로.
4. **Base 클래스 안 상속하고 interface 직접 구현** — SDK가 interface에 메서드를 추가하면 깨짐. 반드시 `*Base`.
5. **Client interceptor와 Worker interceptor 혼동** — 호출 방향이 다름. 클라이언트가 보내는 요청은 `WorkflowClientInterceptor`, 워커 안에서 Workflow가 서버로 보내는 요청은 `WorkflowOutboundCallsInterceptor`.

### 샘플 레퍼런스
- 가장 단순한 inbound/outbound 체인(카운팅): `core/src/main/java/io/temporal/samples/countinterceptor/`
- 트레이싱(OpenTracing/Jaeger): `core/src/main/java/io/temporal/samples/tracing/`
- Signal 기반 재시도 패턴: `core/src/main/java/io/temporal/samples/retryonsignalinterceptor/`
- 특정 Workflow 제외: `core/src/main/java/io/temporal/samples/excludefrominterceptor/`
- 커스텀 어노테이션 interceptor: `core/src/main/java/io/temporal/samples/customannotation/`
- Nexus + MDC 컨텍스트 전파: `core/src/main/java/io/temporal/samples/nexuscontextpropagation/`
- 리플레이 시 interceptor 동작 확인 테스트: `core/src/test/java/io/temporal/samples/interceptorreplaytest/InterceptorReplayTest.java`

---
## 25. `DataConverter` / `PayloadConverter` / `PayloadCodec` — 페이로드 직렬화·암호화

### 역할
Workflow 입출력, Activity 입출력, Signal/Query/Update 인자 등 **모든 payload의 직렬화/역직렬화**를 담당. 기본은 Jackson JSON이지만 암호화·CloudEvents·커스텀 포맷을 끼워넣을 수 있다. **3계층 구조**로 관심사를 분리.

### 3계층 구조

```
Workflow/Activity 코드 (Java Object)
         │
         ▼
┌─────────────────────────────┐
│   DataConverter (최상위)    │  매 호출마다 사용되는 진입점. 보통 CodecDataConverter.
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│   PayloadConverter (직렬화) │  Object ↔ Payload (JSON, protobuf, CloudEvents 등)
└─────────────────────────────┘
         │
         ▼                                 Payload (JSON bytes)
┌─────────────────────────────┐
│   PayloadCodec (변환)       │  Payload → Payload (암호화, 압축, 서명)
└─────────────────────────────┘
         │
         ▼
    Temporal Server (전부 byte array로만 저장)
```

### `DataConverter` — 조립 지점 (`core/.../encodefailures/EncodeFailuresTest.java`)

```java
CodecDataConverter codecDataConverter = new CodecDataConverter(
    DefaultDataConverter.newDefaultInstance(),           // 아래 PayloadConverter 체인
    Collections.singletonList(new SimplePrefixPayloadCodec()),  // 적용할 Codec 리스트
    true);                                               // encode failures도 적용할지

WorkflowClientOptions.newBuilder()
    .setDataConverter(codecDataConverter)
    .build();
```

- **설정 지점은 하나** — `WorkflowClientOptions.setDataConverter(...)` 또는 (테스트) `TestWorkflowRule.setWorkflowClientOptions(...)`.
- **클라이언트와 워커 양쪽에 똑같이 설정해야 함** — 한쪽만 암호화하면 상대가 역직렬화 실패. §1 체크리스트에도 명시.

### `PayloadConverter` — Object ↔ Payload (`core/.../payloadconverter/cloudevents/CloudEventsPayloadConverter.java`)

```java
public class CloudEventsPayloadConverter implements PayloadConverter {
    @Override public String getEncodingType() { return "json/plain"; }

    @Override
    public Optional<Payload> toData(Object value) {
        CloudEvent event = (CloudEvent) value;
        byte[] serialized = CEFormat.serialize(event);
        return Optional.of(Payload.newBuilder()
            .putMetadata("encoding", ByteString.copyFrom(getEncodingType(), UTF_8))
            .setData(ByteString.copyFrom(serialized)).build());
    }

    @Override
    public <T> T fromData(Payload content, Class<T> cls, Type type) {
        return (T) CEFormat.deserialize(content.getData().toByteArray());
    }
}
```

**기본 체인** (`DefaultDataConverter.newDefaultInstance()`에 포함):
1. `NullPayloadConverter` — null
2. `ByteArrayPayloadConverter` — `byte[]`
3. `ProtobufJsonPayloadConverter` — Protobuf (JSON 형식)
4. `ProtobufPayloadConverter` — Protobuf (binary)
5. `JacksonJsonPayloadConverter` — 그 외 전부 (JSON)

**커스텀을 끼워넣는 2가지 방법:**

```java
// 1) override — 같은 encoding type의 기본 converter를 교체
DefaultDataConverter.newDefaultInstance()
    .withPayloadConverterOverrides(new CryptoJacksonJsonPayloadConverter());
// → core/.../payloadconverter/CryptoPayloadConverterTest.java (Jackson Crypto 모듈로 JSON 필드 암호화)

// 2) 완전 교체
new DefaultDataConverter(new NullPayloadConverter(), new CustomXmlPayloadConverter());
```

### `PayloadCodec` — Payload를 또 변환 (주로 암호화) (`core/.../encryptedpayloads/CryptCodec.java`)

```java
public class CryptCodec implements PayloadCodec {
    @Override
    public List<Payload> encode(List<Payload> payloads) {   // Workflow → Server 방향
        return payloads.stream().map(this::encryptPayload).toList();
    }
    @Override
    public List<Payload> decode(List<Payload> payloads) {   // Server → Workflow 방향
        return payloads.stream().map(this::decryptPayload).toList();
    }
}
```

**PayloadConverter와의 차이:**
- **PayloadConverter** — "Object ↔ Payload". 타입을 안다.
- **PayloadCodec** — "Payload ↔ Payload". 타입 모름, 바이트만 다룸. 암호화/압축/서명에 적합.

**장점** — Codec은 PayloadConverter 뒤에 적용되므로, **직렬화 포맷을 바꾸지 않고도** 암호화만 추가할 수 있다.

### 유즈케이스 매트릭스

| 하고 싶은 것 | 레이어 | 샘플 |
| --- | --- | --- |
| 모든 payload 암호화 (end-to-end) | **PayloadCodec** | `core/.../encryptedpayloads/CryptCodec.java` |
| KMS 기반 키 관리 암호화 | **PayloadCodec** | `core/.../keymanagementencryption/awsencryptionsdk/KeyringCodec.java` |
| Workflow 실패도 암호화 | **CodecDataConverter** 3번째 인자 `true` | `core/.../encodefailures/*` |
| JSON 특정 필드만 암호화 | **PayloadConverter override** (Jackson 모듈) | `core/.../payloadconverter/CryptoPayloadConverterTest.java` |
| CloudEvents 형식으로 직렬화 | **PayloadConverter 교체** | `core/.../payloadconverter/cloudevents/*` |
| 간단한 prefix 추가 (디버깅/식별) | **PayloadCodec** | `core/.../encodefailures/SimplePrefixPayloadCodec.java` |

### Codec Server — UI/CLI에서 암호화된 payload 보기

PayloadCodec을 쓰면 Temporal UI와 CLI는 암호화된 바이트만 보여 디버깅이 어렵다. **Codec Server**(HTTP endpoint 로 encode/decode를 노출)를 띄우면 UI가 payload를 그 서버에 보내 복호화해서 표시. 저장소에 Codec Server 샘플은 없지만 [공식 문서](https://docs.temporal.io/dataconversion#codec-server) 참고.

### 자주 하는 실수

1. **클라이언트만 설정, 워커 미설정 (혹은 반대)** — 역직렬화 실패. 반드시 양쪽에 동일 `DataConverter` 설정.
2. **PayloadCodec을 PayloadConverter처럼 사용** — Codec은 타입을 모르니 "암호화만" 하는 게 좋다. 포맷을 바꾸고 싶으면 PayloadConverter override.
3. **encrypt/decrypt 비대칭** — encode에서 metadata를 추가했는데 decode에서 안 읽으면 복호화 실패. 반드시 metadata key로 "이 Codec이 처리한 payload인지" 식별.
4. **키 로테이션 미고려** — key ID를 payload metadata에 저장해, 과거 payload도 과거 키로 복호화 가능하게 설계. `CryptCodec.java`가 `METADATA_ENCRYPTION_KEY_ID_KEY`로 이 패턴 보임.
5. **Workflow 실패 메시지에 민감 정보가 섞여 있는데 codec이 안 걸림** — `CodecDataConverter` 세 번째 인자 `true`로 **failure encoding 포함**해야 함.

### 샘플 레퍼런스
- 기본 암호화 PayloadCodec: `core/src/main/java/io/temporal/samples/encryptedpayloads/CryptCodec.java`
- KMS 기반(AWS Encryption SDK): `core/src/main/java/io/temporal/samples/keymanagementencryption/awsencryptionsdk/EncryptedPayloads.java`, `KeyringCodec.java`
- Failure까지 encode: `core/src/main/java/io/temporal/samples/encodefailures/Starter.java`, `SimplePrefixPayloadCodec.java`, `core/src/test/java/io/temporal/samples/encodefailures/EncodeFailuresTest.java`
- JSON 필드별 암호화(Jackson Crypto 모듈): `core/src/test/java/io/temporal/samples/payloadconverter/CryptoPayloadConverterTest.java`
- CloudEvents 포맷 PayloadConverter: `core/src/main/java/io/temporal/samples/payloadconverter/cloudevents/CloudEventsPayloadConverter.java`, `core/src/test/java/io/temporal/samples/payloadconverter/CloudEventsPayloadConverterTest.java`
- Context propagation(MDC): `core/src/main/java/io/temporal/samples/nexuscontextpropagation/propagation/MDCContextPropagator.java`
