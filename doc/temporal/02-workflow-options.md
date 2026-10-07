# WorkflowOptions

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 6. `WorkflowOptions` — 워크플로 "한 번의 실행"을 정의

### 역할
`WorkflowClient.newWorkflowStub(...)`로 **워크플로를 시작할 때마다** 넘기는 옵션. 서버에 남는 Workflow Execution의 ID, 재시도, 타임아웃, 검색 메타데이터 등 "이 1건의 실행" 수준에서 동작을 결정한다. 샘플에서는 거의 모든 Starter/Controller/Test가 `WorkflowOptions.newBuilder()`로 만든다.

### 생성
```java
GreetingWorkflow workflow =
    client.newWorkflowStub(
        GreetingWorkflow.class,
        WorkflowOptions.newBuilder()
            .setWorkflowId(WORKFLOW_ID)
            .setTaskQueue(TASK_QUEUE)
            .build());
```

### `WorkflowOptions` 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setWorkflowId(String)` | 비즈니스 식별자. 같은 ID로 중복 실행이 가능할지는 아래 reuse policy로 결정 |
| `setTaskQueue(String)` | **필수**. Worker 등록 시 TaskQueue와 반드시 일치해야 함 |
| `setWorkflowRunTimeout(Duration)` | 1회 Run의 최대 실행 시간 (재시도·cron 다음 실행 전까지) |
| `setWorkflowExecutionTimeout(Duration)` | 모든 재시도·cron 체인을 합친 Execution 전체 상한 |
| `setWorkflowTaskTimeout(Duration)` | 단일 Workflow Task 처리 시간 (기본 10s, 보통 그대로 둠) |
| `setWorkflowIdReusePolicy(WorkflowIdReusePolicy)` | 같은 Workflow ID 재사용 정책: `ALLOW_DUPLICATE`, `ALLOW_DUPLICATE_FAILED_ONLY`, `REJECT_DUPLICATE`, `TERMINATE_IF_RUNNING` (`core/.../hello/HelloSignalTest.java`, `.../updatabletimer/DynamicSleepWorkflowStarter.java`) |
| `setRetryOptions(RetryOptions)` | 워크플로 레벨 재시도. 보통 Activity 재시도로 충분해 거의 쓰지 않음 |
| `setCronSchedule(String)` | Cron 식(`"0 * * * *"` 등). 서버가 주기적으로 재시작 (`core/.../hello/HelloCronTest.java`) |
| `setSearchAttributes(Map<String, ?>)` | Temporal UI/List API에서 검색 가능한 attribute. 서버에 미리 등록된 키만 가능 (`core/.../hello/HelloSearchAttributesTest.java`, `.../listworkflows/ListWorkflowsTest.java`) |
| `setTypedSearchAttributes(TypedSearchAttributes)` | 1.26+ 타입 안전 버전. 신규 코드는 이쪽을 권장 |
| `setMemo(Map<String, Object>)` | 검색 불가, UI/API에만 표시되는 메타데이터 |
| `setContextPropagators(List<ContextPropagator>)` | 이 실행에만 다른 컨텍스트 전파기를 사용하고 싶을 때 override |
| `setStartDelay(Duration)` | 시작을 N초 지연 (서버 1.21+). 즉시 스케줄링하지 않고 지연 큐에 넣음 |
| `setDisableEagerExecution(boolean)` | Eager Workflow Task 비활성화. 로컬 워커 즉시 실행 대신 일반 폴링 경로 사용 |
| `setStaticSummary(String)` / `setStaticDetails(String)` | UI에 표시될 사람 친화적 요약·상세 (1.24+) |

> **TIP** — `WorkflowRunTimeout`은 cron 재실행이나 `continueAsNew` 전까지의 개별 Run 상한, `WorkflowExecutionTimeout`은 그 체인 전체 상한이다. 혼동하기 쉬움.

### `WorkflowIdReusePolicy` 심화 — 4가지 동작의 상세 차이

같은 Workflow ID로 **새 Workflow를 시작하려 할 때** 서버가 어떻게 반응할지 결정. "같은 ID의 **이전 Run이 어떤 상태**였는가"에 따라 거부/허용/강제 종료가 달라진다. **중복 실행 방지(멱등성)와 재실행 허용 사이의 트레이드오프**를 서버가 자동으로 처리하게 하는 메커니즘.

> **주의** — 이 policy는 **이전 Run이 이미 종료된 상태**를 기준으로 작동. **이전 Run이 아직 RUNNING**이면 다른 설정(`WorkflowIdConflictPolicy`, 1.24+)이 작동. 아래 "RUNNING과의 상호작용" 절 참고.

#### 각 값의 동작

**`WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE` (기본값)**
- 이전 Run의 결과(COMPLETED / FAILED / CANCELED / TERMINATED / TIMED_OUT) **무관하게** 새 Run 시작 허용.
- 가장 느슨한 정책. "ID는 비즈니스 식별자일 뿐, 재실행은 언제든 OK".
- **쓸 때** — 주문 ID나 일별 배치 ID처럼 "오늘 돌리고 내일 또 돌리는" 경우. 샘플: `core/.../updatabletimer/DynamicSleepWorkflowStarter.java` ("타이머 재설정"에 ID 재사용).

**`WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY`**
- 이전 Run이 **실패류**(FAILED / CANCELED / TERMINATED / TIMED_OUT)로 끝났을 때만 재시작 허용.
- 이전이 COMPLETED면 **거부** → `WorkflowExecutionAlreadyStarted` exception.
- "성공했으면 다시 실행하지 마라, 실패했으면 재시도 OK" 패턴. 비즈니스 레벨 재시도.
- **쓸 때** — 멱등성 보장이 어려운 작업("한 번 성공했으면 다시 돌리면 안 됨") + 실패 복구 허용.

**`WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE`**
- 이전 Run이 **어떤 상태로든 존재하면 거부** → `WorkflowExecutionAlreadyStarted`.
- 가장 엄격. "이 ID는 평생 한 번만".
- **쓸 때** — 결제 ID, 계약 ID처럼 **완전한 1회성** 보장이 필요한 경우. 샘플: `core/.../hello/HelloSignalTest.java`, `.../hello/HelloUpdateTest.java` (테스트 격리에 유용).

**`WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING`** ← 레거시 (1.21+에선 `WorkflowIdConflictPolicy` 권장)
- 이전 Run이 **RUNNING**이면 **먼저 terminate**하고 새 Run 시작.
- 종료된 Run은 `ALLOW_DUPLICATE`처럼 처리.
- **주의** — 서버 버전에 따라 이 값이 RUNNING 상태만 다루고 종료 상태는 `ALLOW_DUPLICATE`와 같이 동작. 최신 서버에선 `WorkflowIdConflictPolicy.TERMINATE_EXISTING`으로 **더 명확하게** 표현.

#### 4가지 선택 비교표

| 이전 Run 상태 | **ALLOW_DUPLICATE** (기본) | **ALLOW_DUPLICATE_FAILED_ONLY** | **REJECT_DUPLICATE** | **TERMINATE_IF_RUNNING** |
| --- | --- | --- | --- | --- |
| 이전 Run 없음 (최초) | O 시작 | O 시작 | O 시작 | O 시작 |
| COMPLETED | **O 시작** | **X 거부** | X 거부 | O 시작 |
| FAILED / CANCELED / TERMINATED / TIMED_OUT | O 시작 | O 시작 | X 거부 | O 시작 |
| **RUNNING** (아래 섹션 참고) | 아래 참고 | 아래 참고 | 아래 참고 | **이전 terminate + 새 시작** |

#### `RUNNING`과의 상호작용 — `WorkflowIdConflictPolicy` (1.24+)

**이전 Run이 RUNNING 상태**일 때는 **별개의 policy**가 작동한다 — `WorkflowOptions.setWorkflowIdConflictPolicy(...)`:

| `WorkflowIdConflictPolicy` 값 | RUNNING일 때 동작 |
| --- | --- |
| `FAIL` (기본) | `WorkflowExecutionAlreadyStarted` 던짐 |
| `USE_EXISTING` | 새 시작 안 하고 **기존 Run의 핸들** 반환 (멱등 호출) |
| `TERMINATE_EXISTING` | 기존 Run terminate 후 새 시작 (레거시 `TERMINATE_IF_RUNNING`을 대체) |

→ **ReusePolicy는 "종료된 과거 Run이 있을 때", ConflictPolicy는 "지금 돌고 있는 Run이 있을 때"**. 두 축이 직교.

#### 샘플 코드 패턴

```java
// 1) 일상적 재실행 — DynamicSleepWorkflowStarter.java
WorkflowOptions.newBuilder()
    .setWorkflowId(DYNAMIC_SLEEP_WORKFLOW_ID)
    .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
    .build();

// 2) 테스트 격리 — HelloSignalTest.java, HelloUpdateTest.java
WorkflowOptions.newBuilder()
    .setWorkflowId(WORKFLOW_ID)
    .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
    .build();

// 3) 멱등 시작 (1.24+)
WorkflowOptions.newBuilder()
    .setWorkflowId(orderId)
    .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.USE_EXISTING)
    .build();
```

#### `continue-as-new`와의 관계

`continue-as-new`로 생성된 새 Run은 **같은 Workflow ID**를 재사용하지만, **ReusePolicy 체크를 거치지 않는다**. 서버가 "체인된 후속 Run"으로 자동 인식. 그래서 `REJECT_DUPLICATE`로 설정해도 continue-as-new는 정상 작동.

#### `signalWithStart` / `updateWithStart`와의 관계

이 두 호출은 "Workflow가 **없으면** 시작, **있으면** Signal/Update". 그래서:
- 이전 Run이 **RUNNING**이면 → 새로 안 시작하고 기존에 신호/업데이트 전달.
- 이전 Run이 **종료**됐으면 → 새 Run 시작, 이때 `ReusePolicy` 적용.

#### 자주 하는 실수

1. **기본값 그대로 두고 "왜 중복 실행이 되지?" 당황** — 기본은 **ALLOW_DUPLICATE**. 1회성 보장이 필요하면 명시적으로 `REJECT_DUPLICATE` 또는 `ALLOW_DUPLICATE_FAILED_ONLY`.
2. **RUNNING 상황을 ReusePolicy로 다루려 함** — 못 다룬다. **`WorkflowIdConflictPolicy`**(1.24+)로 분리해 처리.
3. **`WorkflowExecutionAlreadyStarted` 캐치 안 함** — ID 기반 멱등 시작은 거부 exception을 잡아 "이미 있음"으로 처리해야 함. `DynamicSleepWorkflowStarter.java`가 이 패턴 보여줌.
4. **`TERMINATE_IF_RUNNING`을 "cancel"로 오해** — Terminate는 cleanup 없이 즉시 종료 (§11 ParentClosePolicy의 TERMINATE와 같은 성격). 자식 Workflow 유실, cleanup 유실 가능.
5. **매번 UUID로 ID 생성해 ReusePolicy 의미 없게 만듦** — ReusePolicy는 **ID가 비즈니스 식별자**일 때 의미 있음. UUID면 중복될 일이 없음. 비즈니스 ID(주문번호 등)와 함께 사용.
6. **ReusePolicy로 "현재 실행 중인 중복" 막으려 함** — ReusePolicy는 **과거 Run**만 다룸. 지금 RUNNING은 못 막음. **ConflictPolicy** 필요.

#### 선택 가이드 — 상황별

| 상황 | ReusePolicy | ConflictPolicy (1.24+) |
| --- | --- | --- |
| 일별 배치, 재실행 자유 | `ALLOW_DUPLICATE` (기본) | 상관 없음 |
| 성공한 결제는 다시 실행 금지, 실패 재시도 OK | `ALLOW_DUPLICATE_FAILED_ONLY` | `FAIL` |
| 결제/계약 — 평생 1회만 | `REJECT_DUPLICATE` | `FAIL` |
| 멱등 "주문 생성" — 이미 돌면 기존 핸들 사용 | `ALLOW_DUPLICATE` | `USE_EXISTING` |
| "다시 시작하면 이전 걸 끊어라" (UI 재시도) | `ALLOW_DUPLICATE` | `TERMINATE_EXISTING` |
| 테스트 격리 — 같은 ID 재사용 금지 | `REJECT_DUPLICATE` | `FAIL` |

### 연관 호출
`WorkflowOptions`로 만든 stub은 세 가지 방식으로 호출된다. 호출 방식은 옵션 자체와 독립적이다.
```java
String r = workflow.getGreeting("World");                      // 동기 실행
WorkflowExecution ex = WorkflowClient.start(workflow::greet, x); // 비동기 시작 (리턴은 WorkflowExecution)
CompletableFuture<String> f = WorkflowClient.execute(workflow::getGreeting, "World"); // 비동기 실행 + 결과 Future
```

### `SearchAttributes` 심화 — Workflow Visibility 데이터

Temporal 서버가 **색인(index)으로 유지해 Visibility API(List/Count/Describe)에서 쿼리 가능한 메타데이터**. Workflow를 "원하는 조건으로 찾을 수 있게" 하는 핵심 수단. 예: `CustomerId = "C-1234"`인 Workflow 전부, `Status = "FAILED"`이고 `CreatedBefore < 2026-01-01`인 Workflow 전부.

#### Memo vs SearchAttributes — 자주 혼동

| 항목 | **SearchAttributes** | **Memo** |
| --- | --- | --- |
| 서버가 **색인** | O (쿼리 가능) | **X** (조회만, 쿼리 조건으로 못 씀) |
| 서버에 **사전 등록** 필요 | O (키마다 타입 등록) | X (아무 키나 OK) |
| 사용 가능한 타입 | 제한적 (아래 참고) | 아무 payload (DataConverter로 직렬화) |
| 변경 가능 | O (`upsert`) | **X** (시작 시 1회만 설정, Workflow 안에서 변경 불가) |
| 저장 비용 | 색인 크기 비용 발생 | 거의 없음 (단순 저장) |
| 쓸 때 | **쿼리/필터링 조건** | 변경 안 되는 참고 메타데이터 (UI 설명, 요청 정보) |

→ "List API로 찾아야 한다" → **SearchAttributes**. "UI에 설명으로 보이기만 하면 됨" → **Memo**.

#### 지원 타입 (6가지)

| Java 타입 | `SearchAttributeKey` 생성 | 쿼리 연산자 |
| --- | --- | --- |
| `String` (키워드) | `SearchAttributeKey.forKeyword("Status")` | `=`, `!=`, `IN`, `IS NULL` — **정확 일치**만. LIKE 없음 |
| `String` (텍스트) | `SearchAttributeKey.forText("Description")` | **full-text search** (단어 분해, 부분 일치) |
| `Long` | `SearchAttributeKey.forLong("Count")` | `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN` |
| `Double` | `SearchAttributeKey.forDouble("Amount")` | 숫자 비교 전체 |
| `Boolean` | `SearchAttributeKey.forBoolean("IsPaid")` | `=`, `!=` |
| `OffsetDateTime` | `SearchAttributeKey.forOffsetDateTime("DueAt")` | 날짜 비교 전체 |
| `List<String>` 등 | `SearchAttributeKey.forKeywordList("Tags")` | `IN`, `CONTAINS` |

> **Keyword vs Text 선택** — "C-1234"처럼 **정확 일치**로 찾을 ID는 `forKeyword`. "ultra premium customer"처럼 **단어로 검색**하고 싶으면 `forText`. Keyword는 저장 효율이 좋고 Text는 색인이 크지만 유연함.

#### 서버에 키 사전 등록

운영 서버는 custom search attribute를 **CLI로 등록**해야 사용 가능:

```bash
temporal operator search-attribute create --name CustomerId --type Keyword
temporal operator search-attribute create --name OrderAmount --type Double
temporal operator search-attribute create --name DueAt --type Datetime
```

- 등록 안 하고 `setSearchAttributes`로 넘기면 **서버가 거부** (`InvalidArgument`).
- **테스트 환경**(`TestWorkflowEnvironment`)에선 `registerSearchAttribute(...)`로 코드 안에서 등록.
- **System attribute** (서버가 자동 제공 — `WorkflowId`, `WorkflowType`, `ExecutionStatus`, `StartTime`, `TemporalScheduledById` 등)는 등록 불필요.

#### 사용 1 — Workflow 시작 시 설정

**Legacy (Map 기반)** — `core/.../hello/HelloSearchAttributesTest.java`, `.../listworkflows/ListWorkflowsTest.java`

```java
Map<String, Object> attrs = new HashMap<>();
attrs.put("CustomStringField", customer.getCustomerType());

WorkflowOptions.newBuilder()
    .setSearchAttributes(attrs)
    .setWorkflowId(c.getAccountNum())
    .build();
```

**Typed (1.26+ 권장)** — 컴파일 타임 타입 체크

```java
static final SearchAttributeKey<String> CUSTOMER_ID = SearchAttributeKey.forKeyword("CustomerId");
static final SearchAttributeKey<Long> ORDER_AMOUNT = SearchAttributeKey.forLong("OrderAmount");

WorkflowOptions.newBuilder()
    .setTypedSearchAttributes(
        TypedSearchAttributes.newBuilder()
            .set(CUSTOMER_ID, "C-1234")
            .set(ORDER_AMOUNT, 1500L)
            .build())
    .build();
```

#### 사용 2 — Workflow 안에서 upsert

Workflow 코드가 **진행 중에** 자기 search attribute를 추가/변경하려면 `upsert`. `core/.../customchangeversion/CustomChangeVersionWorkflowImpl.java`가 Workflow 버전 추적에 사용:

```java
static final SearchAttributeKey<String> CUSTOM_CHANGE_VERSION =
    SearchAttributeKey.forKeyword("CustomChangeVersion");

// 값 설정 (덮어쓰기 포함)
Workflow.upsertTypedSearchAttributes(
    CUSTOM_CHANGE_VERSION.valueUnset(),                            // 기존 값 제거
    CUSTOM_CHANGE_VERSION.valueSet("add-v2-activity-change-1"));   // 새 값
```

- `upsert`는 **히스토리에 `UpsertWorkflowSearchAttributes` 이벤트 적재** → 결정적. 리플레이 시 재적용.
- 상태 변경 → 서버 색인 즉시 반영 (최대 수 초 지연 가능).

**Legacy (Map 기반) upsert**

```java
Workflow.upsertSearchAttributes(Map.of("Status", "PROCESSING"));
```

#### Query 문법 (서버 Visibility API)

List API나 `temporal workflow list --query ...`에서 사용:

```sql
-- 특정 고객
CustomerId = "C-1234"

-- 상태 필터
ExecutionStatus = "Running" AND CustomStringField = "VIP"

-- 복합
OrderAmount BETWEEN 1000 AND 5000 AND StartTime > "2026-01-01T00:00:00Z"

-- Keyword 리스트 포함
Tags IN ("urgent", "escalated")

-- NULL 체크
CustomerId IS NULL

-- ORDER BY (일부 백엔드만)
ORDER BY StartTime DESC
```

- SQL과 비슷하지만 **백엔드가 Elasticsearch 또는 SQL DB**인지에 따라 지원 범위 다름.
- 로컬 dev 서버(`temporal server start-dev`)는 **SQLite 기반** → 일부 쿼리 제한.
- 운영 Elasticsearch 백엔드가 full 지원.

#### Workflow 안에서 자기 search attribute 읽기

```java
// Typed
TypedSearchAttributes attrs = Workflow.getTypedSearchAttributes();
String customerId = attrs.get(CUSTOMER_ID);

// Legacy / System attributes
Payload payload = Workflow.getInfo().getSearchAttributes()
    .getIndexedFieldsOrThrow("TemporalScheduledById");
String scheduledBy = GlobalDataConverter.get()
    .fromPayload(payload, String.class, String.class);
```
→ `core/.../hello/HelloSchedules.java`의 Schedule 트리거 추적 패턴 참고.

#### 저장소 유즈케이스 매트릭스

| 유즈케이스 | 샘플 |
| --- | --- |
| 시작 시 설정 | `core/.../hello/HelloSearchAttributesTest.java`, `.../listworkflows/ListWorkflowsTest.java` |
| Workflow 안에서 `upsertTyped`로 추가 | `core/.../customchangeversion/CustomChangeVersionWorkflowImpl.java` |
| System attribute 읽기 (Schedule 트리거 정보) | `core/.../hello/HelloSchedules.java` |
| `List API`로 조회 | `core/.../listworkflows/ListWorkflowsTest.java` (`ListOpenWorkflowExecutionsRequest`) |

#### 자주 하는 실수

1. **키를 서버에 등록 안 하고 바로 사용** — `InvalidArgument`. 운영은 CLI, 테스트는 `registerSearchAttribute`.
2. **Keyword에 긴 문자열 넣고 full-text search 기대** — Keyword는 정확 일치만. 단어 검색은 `forText`.
3. **민감 정보(PII)를 SearchAttribute에 넣음** — 색인에 그대로 저장되고 암호화(§25 PayloadCodec)로 가려지지 않음. 민감 정보는 **Memo + Codec**으로.
4. **대량 데이터 SearchAttribute에 저장** — 색인 크기 폭증. SearchAttribute는 "쿼리 조건으로 쓸 소량의 메타데이터"만.
5. **Memo를 쿼리 조건으로 쓰려 함** — Memo는 색인 안 됨. 쿼리하고 싶으면 SearchAttribute로.
6. **`upsertTypedSearchAttributes` 호출 결과를 즉시 쿼리** — 서버 색인 반영에 수 초 지연. "upsert 직후 List" 로직은 지연 가정.
7. **Local dev 서버에서만 테스트** — SQLite 백엔드 제약으로 Elasticsearch 쿼리가 안 될 수 있음. 복잡한 쿼리는 운영 환경에서 추가 검증.
8. **`SearchAttributeKey`를 매번 새로 만듦** — 성능 손해 + 혼란. **`static final`**로 상수화. `customchangeversion` 샘플이 올바른 패턴.

#### Legacy Map vs Typed — 마이그레이션

- **1.26+ 신규 코드**: `TypedSearchAttributes` + `SearchAttributeKey`
- **기존 코드**: `Map<String, Object>` + `upsertSearchAttributes` 그대로 작동 (deprecated 아님)
- **같은 Workflow 안에서 혼용** 가능하지만 혼란. 신규 Workflow는 Typed 통일 권장.

### `Memo` 심화 — 색인 없는 참고 메타데이터

색인되지 **않는** 자유 형식 메타데이터. UI/`Describe` API에서 Workflow 정보 조회 시 함께 반환되지만 **쿼리 조건으로는 못 쓴다**. "변경되지 않는 참고 정보"에 적합.

> 저장소 샘플에 `setMemo` 전용 예는 없음. SDK API 자체는 안정적이며, 본 설명은 `WorkflowOptions.setMemo(Map)` API 기준.

#### SearchAttributes와의 차이 재강조

§6의 "SearchAttributes 심화" 상단 비교표를 다시 요약:

| 질문 | → **Memo**가 맞음 | → **SearchAttributes**가 맞음 |
| --- | --- | --- |
| "이 조건으로 Workflow 리스트 조회?" | **X** (색인 안 됨 → 못 함) | **O** |
| "Workflow 열어보면 보이면 됨" | **O** | O |
| "서버에 키를 먼저 등록해야 함?" | **X** (등록 불필요) | O |
| "임의 Java 객체 저장 가능?" | **O** (DataConverter로 직렬화) | X (6개 타입 제한) |
| "Workflow 안에서 수정 가능?" | 서버 1.25+부터 **upsertMemo 제한적 지원** (초기엔 X) | O (`upsertTypedSearchAttributes`) |
| "PII/민감 정보 포함 가능?" | O (DataConverter + Codec으로 암호화 가능) | **X** (색인에 평문) |

#### 사용 — Workflow 시작 시 설정

```java
Map<String, Object> memo = new HashMap<>();
memo.put("requestedBy", "ops-team");
memo.put("ticketUrl", "https://jira.example.com/ORD-1234");
memo.put("requestPayload", originalRequest);           // 임의의 복잡한 객체도 OK

WorkflowOptions.newBuilder()
    .setWorkflowId("order-" + id)
    .setTaskQueue(TASK_QUEUE)
    .setMemo(memo)
    .build();
```

- `Map<String, Object>` 받음. 값은 **`DataConverter`로 직렬화**되는 아무 타입.
- 서버는 **payload를 그대로 저장**하고 색인 없음 → 저장 비용 작음, 쿼리 불가.

#### 사용 — ChildWorkflow에도 지정 가능

```java
ChildWorkflowOptions.newBuilder()
    .setWorkflowId(childId)
    .setMemo(Map.of("parent", parentId, "reason", "fan-out partition"))
    .build();
```

#### Workflow 안에서 읽기

```java
Map<String, Payload> memo = Workflow.getInfo().getMemo();
Payload p = memo.get("requestedBy");
String val = GlobalDataConverter.get().fromPayload(p, String.class, String.class);
```
- `Workflow.getInfo().getMemo()`는 **직렬화된 Payload 형태**를 반환. `DataConverter`로 역직렬화 필요.
- Workflow 코드에서 Memo를 **읽는 건 결정적** (히스토리에서 재주입).

#### Workflow 밖에서 읽기

```java
// 1) WorkflowStub.describe() (Workflow 종료 후에도 조회 가능)
WorkflowExecutionDescription desc = workflowStub.describe();
// desc.getMemo()에서 꺼내 역직렬화

// 2) Visibility List API — Memo는 리스트 응답에 포함돼 UI에 표시됨
// 단, "Memo 조건으로 필터"는 못 함
```

#### 변경 — `upsertMemo` (서버/SDK 버전 의존)

- **최신 SDK/서버** — `Workflow.upsertMemo(Map<String, Object>)` 지원 (null 값으로 삭제).
- **구 버전** — Memo는 Workflow 시작 시 1회 설정, 이후 변경 불가.
- 가변 상태는 **SearchAttribute의 `upsertTypedSearchAttributes`**가 더 안정적 (쿼리까지 되고 지원도 오래됨).

#### 저장 크기 제한

- Memo 1개 Workflow 전체에 대해 **총 ~2 MB** (서버 설정에 따라 다름). SearchAttribute보다 느슨하지만 **큰 payload는 피해야 함** — 큰 데이터는 외부 저장소 + 참조(URL)만 Memo로.
- 참조 패턴: `memo.put("snapshotUrl", "s3://bucket/key")`

#### 유즈케이스 매트릭스

| 상황 | 선택 |
| --- | --- |
| 디버깅/감사 용 "누가 왜 시작했나" | **Memo** (`requestedBy`, `ticketUrl`) |
| List API 쿼리 조건 ("고객 X의 모든 Workflow") | **SearchAttributes** |
| 민감 원본 요청 payload (PII 포함) | **Memo + PayloadCodec** 암호화 |
| 외부 시스템 참조 (JIRA 티켓, S3 경로) | **Memo** |
| 운영 중 상태 변화 추적 ("현재 PROCESSING") | **SearchAttributes** (`upsertTypedSearchAttributes`) |
| UI에 요약 설명 표시 | `WorkflowOptions.setStaticSummary(...)` (§6) 또는 **Memo** |

#### 자주 하는 실수

1. **Memo를 쿼리 조건으로 쓰려 함** — 못 함. 쿼리하고 싶으면 SearchAttribute.
2. **큰 payload를 Memo에 통째로** — 저장 비용과 UI 응답 지연. 외부 저장소 + 참조로.
3. **SearchAttribute처럼 "운영 중 상태 변화 추적"에 사용** — 구 버전 SDK/서버는 변경 자체가 안 됨. 가변 상태는 SearchAttribute.
4. **민감 정보 평문 저장** — Memo는 색인은 안 되지만 **payload 자체는 평문**. PII는 반드시 **PayloadCodec**으로 암호화(§25).
5. **Memo를 Workflow 로직 분기에 사용** — Memo는 **메타데이터**지 비즈니스 입력이 아님. 로직 조건은 WorkflowMethod 인자나 Signal/Update로.

#### Memo vs SearchAttributes vs Static Summary — 3자 정리

| 수단 | 색인 쿼리 | UI 표시 | 변경 | 타입 자유도 | 비용 |
| --- | --- | --- | --- | --- | --- |
| **SearchAttributes** | **O** | O | O (`upsert`) | 6개 타입 | 색인 비용 |
| **Memo** | X | O | 제한적 | 자유 | 작음 |
| **Static Summary/Details** (`setStaticSummary`) | X | **O** (UI 전용) | X | String만 | 거의 없음 |

→ Workflow를 UI에서 **"잘 보이게"**만 하려면 `setStaticSummary` + `setStaticDetails`가 가장 가볍고, 참고 metadata가 많으면 Memo, 쿼리 조건이면 SearchAttributes.

### `StaticSummary` / `StaticDetails` 심화 — UI 표시 전용 설명 (1.24+)

**Temporal UI와 CLI 출력**에서 Workflow(또는 Timer / Activity / Nexus operation)를 **사람이 알아보기 쉽게 설명**하는 자유 텍스트. 비즈니스 로직엔 아무 영향 없고 **운영자/디버거를 위한 UX**가 유일한 목적.

> 저장소에는 **`TimerOptions.setSummary(...)`** 1개 사례만 있음 (`core/.../hello/HelloWorkflowTimer.java`). Workflow·ChildWorkflow·Activity·Nexus 쪽 `setStaticSummary`/`setSummary`는 SDK API 자체는 안정적이나 샘플에선 미시연.

#### 어디에 설정 가능한가 — 4곳

| 설정 지점 | API | UI에서 보이는 곳 |
| --- | --- | --- |
| **Workflow 시작** | `WorkflowOptions.setStaticSummary(String)` / `.setStaticDetails(String)` | Workflow Execution 요약 패널 |
| **ChildWorkflow 시작** | `ChildWorkflowOptions.setStaticSummary(...)` / `.setStaticDetails(...)` | Child Workflow 섹션 |
| **Activity 호출** | `ActivityOptions.setSummary(String)` | 각 Activity의 라인 요약 |
| **Timer 생성** | `TimerOptions.setSummary(String)` | 타임라인에서 각 Timer 라벨 |
| **Nexus operation** | `NexusOperationOptions.setSummary(String)` | Nexus 호출 요약 |

> **이름 차이** — Workflow·ChildWorkflow는 **`setStaticSummary`/`setStaticDetails`** (2개 필드), Activity·Timer·Nexus는 **`setSummary`** (1개). 모두 같은 UI 목적.

#### Summary vs Details

| 필드 | 용도 | 길이 가이드 |
| --- | --- | --- |
| **Summary** | **한 줄**로 "이 Workflow가 뭐 하는지" | 짧게 (~1-2줄) — UI 리스트/타임라인 라벨 |
| **Details** | 긴 설명, 디버깅 메모, 외부 참조 | 길어도 됨 (Markdown 가능) — 상세 패널에서 펼침 |

#### 사용 예

**Workflow (가장 흔한 유즈케이스)**

```java
WorkflowOptions.newBuilder()
    .setWorkflowId("order-" + orderId)
    .setTaskQueue(TASK_QUEUE)
    .setStaticSummary("Order #" + orderId + " for " + customer.getName())
    .setStaticDetails(
        "**Customer**: " + customer.getName() + "\n" +
        "**Email**: " + customer.getEmail() + "\n" +
        "**Items**: " + items.size() + "\n" +
        "[JIRA ticket](" + ticketUrl + ")")
    .build();
```
→ UI 리스트에서 "Order #1234 for John"으로 요약이 보이고, 상세 패널엔 Markdown 렌더된 긴 설명.

**Timer** (`HelloWorkflowTimer.java`)

```java
Workflow.newTimer(
    Duration.ofSeconds(30),
    TimerOptions.newBuilder().setSummary("Workflow Timer").build());
```
→ UI 타임라인에서 Timer 노드에 "Workflow Timer" 라벨이 붙어, 여러 Timer가 섞여 있을 때 "어느 게 어느 거인지" 즉시 식별.

**Activity**

```java
ActivityOptions.newBuilder()
    .setStartToCloseTimeout(Duration.ofMinutes(2))
    .setSummary("Charge credit card for order")
    .build();
```
→ Activity 라인에 "Charge credit card for order" 보여 100개 Activity 중에서도 빠르게 찾음.

**Nexus**

```java
NexusOperationOptions.newBuilder()
    .setScheduleToCloseTimeout(Duration.ofSeconds(30))
    .setSummary("Fraud check via risk-service")
    .build();
```

#### Markdown 지원 (Workflow Details)

Temporal UI의 Workflow Details 패널은 **Markdown 렌더링**을 지원:

```java
.setStaticDetails(
    "## Order Pipeline\n" +
    "- Customer: **" + name + "**\n" +
    "- Items: " + count + "\n\n" +
    "| Field | Value |\n" +
    "|---|---|\n" +
    "| Priority | " + priority + " |\n\n" +
    "External: [Fulfillment](https://admin.example.com/orders/" + id + ")")
```
→ 운영자가 Workflow 열자마자 **상황 파악 + 외부 시스템 링크 클릭 이동**까지 한 번에.

#### 현재 상태 변화 추적 — `setCurrentDetails` / `upsertSummary` (실험적/최신)

Static Summary/Details는 **시작 시 고정**. "진행 중 상태를 반영"하고 싶으면 (최신 서버/SDK에서 지원):

- `Workflow.setCurrentDetails(String)` — 현재 진행 상태를 Markdown으로 지속 업데이트. 예: "Step 3/5: Charging credit card"
- Activity 쪽은 `Activity.getExecutionContext().setCurrentDetails(...)`로 heartbeat와 함께 업데이트 가능

→ 가변 설명이 필요하면 **SearchAttribute**(`upsertTypedSearchAttributes`로 구조화) 또는 `setCurrentDetails`.

#### Memo / SearchAttributes / StaticSummary 선택 가이드

| 질문 | 선택 |
| --- | --- |
| "UI 열어서 '이 Workflow 뭐지' 한 줄 요약" | **StaticSummary** |
| "운영자가 열면 긴 Markdown 설명 + 외부 링크" | **StaticDetails** |
| "쿼리 조건으로 Workflow 리스트 검색" | **SearchAttributes** |
| "쿼리는 안 하지만 구조화된 참고 metadata (customer info 등)" | **Memo** |
| "실행 중 상태가 바뀜 ('현재 Step 3')" | **`setCurrentDetails`** 또는 **SearchAttributes `upsert`** |
| "임의 Java 객체(큰 payload)" | **Memo** |

#### 운영 가치 — 왜 챙겨두면 좋은가

- **인시던트 대응** — 수백 개 Workflow 중 "어느 게 문제인지" UI 리스트에서 즉시 판별.
- **디버깅** — Workflow Details에 "입력 요청 ID + JIRA 티켓 + 외부 시스템 링크" 넣어두면 1클릭으로 원인 찾기.
- **감사(audit)** — "왜 이 Workflow가 시작됐는지" 흔적 보존 (단, 변경 안 되는 사실만).
- **공유** — 비기술자가 UI 열어도 Workflow 의미 파악 가능.

#### 자주 하는 실수

1. **민감 정보 포함** — Markdown Details에 PII 넣으면 UI 접근자 모두에게 노출. Static이므로 **Codec 암호화도 안 됨**. 민감 정보는 Memo + Codec으로.
2. **매우 긴 string 넣기** — 서버 저장과 UI 응답 비용. 긴 설명은 외부 URL 링크로.
3. **"상태 변화 반영"에 Static Summary 사용** — Static은 시작 시 고정. 가변은 `setCurrentDetails` 또는 SearchAttribute.
4. **UI 표시 외 로직에 의존** — Static Summary는 비즈니스 로직에서 읽지 **말 것**. 그런 용도는 Memo/SearchAttribute.
5. **ChildWorkflow 요약 생략** — 수십 개 자식이 있으면 UI에서 구분이 어려움. 자식마다 `setStaticSummary("partition " + i)` 같은 라벨 권장 (`SlidingWindowBatchWorkflowImpl.java`의 `WorkflowId` 포맷팅과 같은 맥락).

#### 저장소에서의 현재 활용

| 샘플 | 사용 |
| --- | --- |
| `core/.../hello/HelloWorkflowTimer.java` | `TimerOptions.setSummary("Workflow Timer")` — 타임라인 식별 |

→ 신규 Workflow 작성 시 **최소한 `setStaticSummary` 한 줄**이라도 넣는 걸 운영 친화적 기본 습관으로.
