# 시작하기 — 서버 설치, 전체 흐름, MSA 매핑

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 0. 사전 준비 — Temporal Server 설치/실행

샘플 코드는 **`127.0.0.1:7233`에서 Temporal Server가 떠 있어야** 동작한다. 서버가 없으면 다음 에러가 난다.

```
io.grpc.netty.shaded.io.netty.channel.AbstractChannel$AnnotatedConnectException:
    Connection refused: no further information: /127.0.0.1:7233
```

로컬 개발용 서버는 Temporal CLI(`temporal` 바이너리)에 포함된 `start-dev` 명령으로 띄운다.

### 설치 (Windows)

**방법 A — 공식 PowerShell 설치 스크립트 (권장)**

```powershell
iwr https://temporal.download/cli.ps1 -useb | iex
```

- `%USERPROFILE%\.temporalio\bin\temporal.exe`에 설치, User PATH 자동 등록.
- 설치 직후엔 **새 PowerShell 창**을 열어야 PATH가 반영된다.
- 스크립트 호스트가 간헐적으로 Cloudflare 522를 뱉으면 방법 B/C로 대체.

**방법 B — GitHub Releases에서 수동 다운로드**

1. https://github.com/temporalio/cli/releases/latest 에서 `temporal_cli_<버전>_windows_amd64.zip` 받기.
2. 압축을 풀어 `temporal.exe`를 PATH 걸린 폴더(예: `%USERPROFILE%\.temporalio\bin\`)에 둔다.
3. 해당 폴더를 User 환경변수 `Path`에 추가.

PowerShell 한 번에 수행:

```powershell
$ProgressPreference = 'SilentlyContinue'
$rel = iwr https://api.github.com/repos/temporalio/cli/releases/latest -UseBasicParsing | ConvertFrom-Json
$asset = $rel.assets | Where-Object { $_.name -like 'temporal_cli_*_windows_amd64.zip' } | Select-Object -First 1
$zip = "$env:TEMP\$($asset.name)"
iwr $asset.browser_download_url -OutFile $zip -UseBasicParsing
$binDir = "$HOME\.temporalio\bin"
New-Item -ItemType Directory -Force -Path $binDir | Out-Null
Expand-Archive -Path $zip -DestinationPath $binDir -Force
$userPath = [Environment]::GetEnvironmentVariable('Path','User')
if ($userPath -notlike "*$binDir*") {
    [Environment]::SetEnvironmentVariable('Path', ($userPath.TrimEnd(';') + ";$binDir"), 'User')
}
$env:Path = "$env:Path;$binDir"
temporal --version
```

**방법 C — Scoop**

```powershell
scoop install temporal
```

> `winget install temporal.cli`은 공식 저장소에 패키지가 없어 실패한다.

### 실행

새 PowerShell 창에서:

```powershell
temporal server start-dev
```

| 엔드포인트 | 용도 |
| --- | --- |
| `127.0.0.1:7233` | gRPC — 샘플 코드(`WorkflowServiceStubs`)가 접속하는 곳 |
| http://127.0.0.1:8233 | Web UI — 워크플로/태스크큐/히스토리 조회 |

이 창은 **계속 띄워 둔 채로** 다른 창(또는 IntelliJ)에서 샘플을 실행한다. 기본 저장소는 in-memory라 종료 시 히스토리가 사라진다. 영속화하려면:

```powershell
temporal server start-dev --db-filename temporal.db
```

### 동작 확인

```powershell
temporal operator cluster health
```

또는 http://127.0.0.1:8233 접속. 둘 다 OK면 샘플 실행에서 `Connection refused`가 사라진다.

---

## 1. 전체 흐름 한눈에 보기

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

## 1-A. MSA 호출과의 매핑 — Spring WebClient/RestClient와 비교

Spring MSA에서 다른 서비스를 부를 때는 WebClient/RestClient로 HTTP를 직접 쏘지만, Temporal은 **pod 간 직접 통신이 아예 없다**. 모든 호출이 Temporal Server를 경유한다.

```
[Spring MSA]
Service A pod ───HTTP──► Service B pod       (B가 떠있어야 함, 서비스 디스커버리 필요)

[Temporal]
Service A pod ──gRPC──► Temporal Server ──TaskQueue──► Service B pod
                            │
                            └─ B pod이 당장 없으면 Task를 **큐에 보관**
                               → B pod이 뜨면 polling으로 가져감
```

### 개념 매핑

| Spring MSA | Temporal |
| --- | --- |
| 서비스 호스트/포트 (`http://service-b:8080`) | **TaskQueue 이름** (`"ServiceBTaskQueue"`) |
| 서비스 디스커버리 (Eureka, k8s DNS) | **불필요** — TaskQueue는 그냥 문자열, 서버가 라우팅 |
| 로드밸런서 | **불필요** — 서버가 polling 중인 Worker들에게 Task 분배 |
| 서킷브레이커·재시도 (Resilience4j) | **SDK 기본 제공** — Activity/Workflow 레벨 `RetryOptions` (§8) |
| WebClient 호출 (B 다운 시 즉시 실패) | 호출은 **항상 성공** — Server가 Task 영속화, B 복구 시 처리 |
| OpenAPI 스펙 공유 | `@WorkflowInterface` 인터페이스 jar 공유 (또는 untyped 호출) |
| 타임아웃 (`WebClient.responseTimeout`) | `WorkflowOptions.setWorkflowRunTimeout` (§6), `ActivityOptions.setStart/Schedule...Timeout` (§7) |
| 분산 트레이싱 (Sleuth/Micrometer) | `OpenTracingClientInterceptor` + Workflow Interceptor (§24) |

### 호출 코드 패턴

**케이스 1 — 다른 서비스의 Workflow를 Starter에서 시작** (REST의 `POST /orders`에 해당)

```java
// Service A에서 — 같은 Temporal Server를 바라보는 WorkflowClient
WorkflowClient client = ...;

// (a) typed: B의 @WorkflowInterface jar를 공유받아서 호출
OrderWorkflow stub = client.newWorkflowStub(
    OrderWorkflow.class,
    WorkflowOptions.newBuilder()
        .setTaskQueue("OrderServiceTaskQueue")          // ← B의 TaskQueue
        .setWorkflowId("order-" + orderId)
        .build());
String result = stub.placeOrder(req);                   // 결과까지 블로킹 (동기)
// 또는 WorkflowClient.start(stub::placeOrder, req);    // fire-and-forget

// (b) untyped: jar 공유 없이 workflow type 이름(문자열)으로 호출 — REST처럼 느슨 결합
WorkflowStub untyped = client.newUntypedWorkflowStub(
    "OrderWorkflow",
    WorkflowOptions.newBuilder()
        .setTaskQueue("OrderServiceTaskQueue")
        .setWorkflowId("order-" + orderId)
        .build());
untyped.start(req);
String result = untyped.getResult(String.class);
```

→ A는 **B pod이 지금 떠있는지 모른다**. Temporal Server가 Task를 큐에 쌓고, B의 Worker가 polling으로 가져가 처리.

**케이스 2 — Workflow 안에서 다른 서비스를 호출** (Workflow Composition — 세 가지 선택지)

```java
// 2-a) Child Workflow — 다른 TaskQueue 지정 → 다른 서비스의 Worker가 실행 (§11)
PaymentWorkflow child = Workflow.newChildWorkflowStub(
    PaymentWorkflow.class,
    ChildWorkflowOptions.newBuilder()
        .setTaskQueue("PaymentServiceTaskQueue")
        .build());
PaymentResult r = child.charge(amount);

// 2-b) External Workflow — 이미 돌고 있는 다른 Workflow에 Signal (§17)
ExternalWorkflowStub ext = Workflow.newExternalWorkflowStub(
    OtherWorkflow.class, "other-workflow-id");
ext.someSignal(data);

// 2-c) Activity만 다른 서비스로 — Workflow 자체는 A에서 돌리고 Activity만 B 전용 Worker에서 실행 (§7)
PaymentActivity act = Workflow.newActivityStub(
    PaymentActivity.class,
    ActivityOptions.newBuilder()
        .setTaskQueue("PaymentServiceTaskQueue")
        .setStartToCloseTimeout(Duration.ofSeconds(30))
        .build());
act.charge(amount);
```

**케이스 3 — Nexus** — "서비스 간 RPC"를 위한 공식 추상화 (§13)

- 다른 **namespace**(심지어 다른 cluster)의 workflow를 "서비스 operation"처럼 호출
- RPC 느낌으로 쓰고 싶다면 Nexus가 가장 가깝다
- 샘플: `core/.../nexuscancellation/`, `core/.../nexusmessaging/`

### REST 대비 얻는 것

1. **Target 서비스가 다운돼도 호출이 실패하지 않는다** — Server가 Task 영속화 → Worker가 돌아올 때 처리. 재시도/서킷브레이커 코드가 거의 사라짐.
2. **서비스 디스커버리 불필요** — TaskQueue 이름만 공유.
3. **네트워크 토폴로지 단순화** — 모든 pod은 **Temporal Server로 outbound만** 열면 끝. pod 간 inbound 포트 노출 없음.
4. **스케일링이 Worker polling 기반** — 바쁘면 pod 추가 → 자동으로 더 당겨감. LB 설정 변경 없음.
5. **호출이 영구히 추적된다** — WorkflowId로 지금 어디서 뭐 하고 있는지 Web UI(http://127.0.0.1:8233)에서 조회.

### 트레이드오프 (REST 대비 불편한 점)

- **짧은 동기 조회엔 과하다** — 단순 Read API는 REST/gRPC가 낫다. Temporal은 **오래 걸리는 비즈니스 프로세스 + 재시도/보상/상태관리**가 필요한 호출에 쓴다.
- **Starter가 interface(또는 workflow type 이름)를 알아야** typed 호출 가능 — jar 공유 전략을 정해야 한다.
- **Payload 크기 제한** (기본 2MB) — 큰 바이너리는 S3 pre-signed URL 등으로 우회.
- **디버깅 멘탈 모델이 다름** — HTTP trace 대신 **Workflow History**를 본다 (Web UI가 그걸 지원).
- **동기 응답 UX** — "요청 보내고 결과 받기"가 필요한 경우 `stub.method()` 직접 호출로 블로킹하거나, Update(§19)·updateWithStart로 "시작과 결과 반환"을 한 번에 묶는 패턴을 쓴다.

### 샘플 레퍼런스

- 다른 TaskQueue로 Activity 분리: `core/src/main/java/io/temporal/samples/fileprocessing/`
- Child Workflow (조합): `core/src/main/java/io/temporal/samples/hello/HelloChild.java`
- External Workflow Signal: `core/src/main/java/io/temporal/samples/updatabletimer/`
- Nexus (크로스 namespace RPC): `core/src/main/java/io/temporal/samples/nexuscancellation/`, `.../nexusmessaging/`
