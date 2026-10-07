# Client/Worker 셋업 — Stubs ~ Worker

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 2. `WorkflowServiceStubs` — gRPC 커넥션

### 역할
Temporal Server(기본 `127.0.0.1:7233`)와의 **gRPC 채널 그 자체**. 저수준 proto API(`WorkflowServiceGrpc`, `OperatorServiceGrpc`)를 감싼다. JVM당 보통 1개만 만들어 공유한다. 비용이 크기 때문에 재사용이 중요.

### 생성
```java
WorkflowServiceStubs service =
    WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions());
```

샘플에서는 `ClientConfigProfile.load()`가 env/파일 기반으로 Options를 만들어주므로 host/namespace를 하드코딩하지 않는다.

### `WorkflowServiceStubsOptions` 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setTarget(String)` | 서버 주소. 예: `127.0.0.1:7233`, `my-ns.tmprl.cloud:7233` |
| `setSslContext(SslContext)` | mTLS/Temporal Cloud 연결 시 Netty `SslContext` 주입 (`core/.../ssl/Starter.java` 참고) |
| `setEnableHttps(boolean)` | HTTPS 사용 여부 |
| `setChannel(ManagedChannel)` | 외부에서 만든 gRPC 채널을 그대로 사용 |
| `setRpcTimeout(Duration)` | 단일 RPC 호출 기본 타임아웃 (기본 10s) |
| `setRpcLongPollTimeout(Duration)` | Long-poll RPC 타임아웃 (기본 60s) — 워커가 TaskQueue를 폴링할 때 사용 |
| `setRpcQueryTimeout(Duration)` | Query RPC 전용 타임아웃 |
| `setRpcRetryOptions(RpcRetryOptions)` | gRPC 레벨 재시도 정책 |
| `setConnectionBackoffResetFrequency(Duration)` | 재연결 백오프 리셋 주기 |
| `setGrpcReconnectFrequency(Duration)` | 강제 재연결 주기 (keep-alive 보조) |
| `setMetricsScope(Scope)` | Micrometer/Prometheus 등 메트릭 scope 주입 |
| `setHeaders(Metadata)` | 공통 gRPC 헤더 (`Authorization: Bearer …` 등) |
| `setApiKey(Supplier<String>)` | Temporal Cloud API Key 공급자 |
| `setKeepAliveTime/Timeout(...)` | TCP keep-alive 튜닝 |

> **TIP** — Temporal Cloud는 `setTarget(host:7233)` + `setSslContext(...)` 또는 `setApiKey(...)` 조합이 일반적.

---

## 3. `WorkflowClient` — 워크플로 조작 API

### 역할
Stubs 위에 올라가는 **고수준 클라이언트**. 워크플로를 시작(start/execute), Signal/Query 전송, 결과 조회, 히스토리 조회 등을 담당. 데이터 직렬화(`DataConverter`), 네임스페이스, 인터셉터 체인도 여기서 결정된다.

### 생성
```java
WorkflowClient client =
    WorkflowClient.newInstance(service, profile.toWorkflowClientOptions());
```

### `WorkflowClientOptions` 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setNamespace(String)` | 접속할 Temporal namespace (기본 `default`) |
| `setDataConverter(DataConverter)` | 페이로드 직렬화기. 암호화/CloudEvents/커스텀 변환 적용 (`core/.../payloadconverter/*` 참고) |
| `setInterceptors(WorkflowClientInterceptor...)` | 클라이언트 호출 인터셉터. 트레이싱/카운팅/인증 메타데이터 삽입 (`core/.../countinterceptor/InterceptorStarter.java`) |
| `setIdentity(String)` | 서버 UI에 표시될 클라이언트 식별자 |
| `setBinaryChecksum(String)` | 레거시 Worker Versioning용 체크섬 (최신은 `WorkerDeploymentOptions` 사용) |
| `setContextPropagators(List<ContextPropagator>)` | MDC/트레이싱 컨텍스트를 서버 왕복에 포함 |
| `setQueryRejectCondition(QueryRejectCondition)` | 종료된 워크플로 Query 거부 정책 |

### 클라이언트 사용 예
```java
GreetingWorkflow stub =
    client.newWorkflowStub(
        GreetingWorkflow.class,
        WorkflowOptions.newBuilder()
            .setWorkflowId(WORKFLOW_ID)
            .setTaskQueue(TASK_QUEUE)
            .build());
String result = stub.getGreeting("World");
```
`newWorkflowStub(...)`는 클라이언트 측에서 워크플로를 **시작/연결**하기 위한 프록시다. `WorkflowOptions`는 "이 워크플로 실행 1건"의 ID/TaskQueue/재시도/타임아웃을 정한다 — 여기서는 다루지 않는다.

---

## 4. `WorkerFactory` — 워커 공용 관리자

### 역할
동일 JVM 안의 **모든 Worker가 공유**하는 리소스(스레드풀, 캐시, 인터셉터, 메트릭)를 관리하는 팩토리. 반드시 `WorkflowClient`로 생성하고, 마지막에 `factory.start()`를 호출해야 등록된 워커들이 폴링을 시작한다.

### 생성
```java
WorkerFactory factory = WorkerFactory.newInstance(client);
// 또는
WorkerFactory factory = WorkerFactory.newInstance(client, workerFactoryOptions);
```

### `WorkerFactoryOptions` 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setWorkerInterceptors(WorkerInterceptor...)` | 모든 워커의 Workflow/Activity 실행에 적용할 인터셉터 (트레이싱/카운팅/정책 주입) |
| `setWorkflowCacheSize(int)` | **스티키 캐시**에 유지할 워크플로 수. 캐시 miss 시 전체 히스토리 리플레이 발생 |
| `setWorkflowHostLocalPollThreadCount(int)` | 호스트 로컬 Workflow Task 폴러 스레드 수 |
| `setMaxWorkflowThreadCount(int)` | JVM 전체에서 동시 실행 가능한 **워크플로 스레드 총수** (워크플로는 가상 스레드처럼 멀티플렉싱됨) |
| `setEnableLoggingInReplay(boolean)` | 히스토리 리플레이 중에도 로그 출력 (디버깅용) |
| `validateAndBuildWithDefaults()` | 기본값 검증 포함 빌드 (샘플 관례) |

> 실 예시: `core/.../countinterceptor/InterceptorStarter.java`, `core/.../excludefrominterceptor/RunMyWorkflows.java`, `core/.../tracing/TracingTest.java`.

---

## 5. `Worker` — TaskQueue 하나를 처리하는 폴러

### 역할
특정 **TaskQueue 하나**를 폴링하면서 Workflow Task와 Activity Task를 실행한다. 하나의 `WorkerFactory`에서 TaskQueue마다 Worker를 여러 개 만들 수 있다.

### 생성과 등록
```java
Worker worker = factory.newWorker(TASK_QUEUE);
// 또는
Worker worker = factory.newWorker(TASK_QUEUE, workerOptions);

worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);
worker.registerActivitiesImplementations(new GreetingActivitiesImpl());

factory.start();   // 이 시점부터 실제 폴링 시작
```
- Workflow 구현체는 **클래스**(`.class`)를 등록 — SDK가 매 Workflow Task마다 새 인스턴스를 생성.
- Activity 구현체는 **인스턴스**를 등록 — stateless/thread-safe 전제, 모든 요청이 공유.

### `WorkerOptions` 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setMaxConcurrentActivityExecutionSize(int)` | 이 워커에서 동시 실행 가능한 Activity 수 (기본 200) |
| `setMaxConcurrentWorkflowTaskExecutionSize(int)` | 동시 처리할 Workflow Task 수 (기본 200) |
| `setMaxConcurrentLocalActivityExecutionSize(int)` | Local Activity 동시 실행 수 |
| `setMaxWorkerActivitiesPerSecond(double)` | 워커 단위 Activity 처리율 상한 |
| `setMaxTaskQueueActivitiesPerSecond(double)` | **TaskQueue 전체**(모든 워커 합산) Activity 처리율 상한 — 서버가 조정 |
| `setMaxConcurrentActivityTaskPollers(int)` | Activity Task 폴러 스레드 수 (기본 5) |
| `setMaxConcurrentWorkflowTaskPollers(int)` | Workflow Task 폴러 스레드 수 (기본 5) |
| `setStickyTaskQueueDrainTimeout(Duration)` | 종료 시 스티키 TaskQueue 비우기 대기 시간 |
| `setDefaultDeadlockDetectionTimeout(long)` | 워크플로 코드 블로킹 감지 임계값 (ms) |
| `setBuildId(String)` | 워커 버전 식별자 (레거시 Worker Versioning) |
| `setDeploymentOptions(WorkerDeploymentOptions)` | **최신 Worker Versioning**. 버전·배포명 지정 (`core/.../workerversioning/*` 참고) |
| `setLocalActivityWorkerOnly(boolean)` | 이 워커를 Local Activity 전용으로 돌림 |
| `setDisableEagerExecution(boolean)` | Eager Workflow Task 실행 비활성화 |
