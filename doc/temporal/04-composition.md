# Workflow 조합 — Saga / Child / Nexus

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 10. `Saga.Options` — 보상 트랜잭션(SAGA) 설정

### 역할
`io.temporal.workflow.Saga`는 워크플로 안에서 **보상 가능한 단계들을 순서대로 등록**하고, 실패 시 역순(또는 병렬)으로 보상(compensation) 함수를 실행해주는 헬퍼. `Saga.Options`는 그 보상 실행 방식을 정한다. 저장소에 `bookingsaga`, `bookingsyncsaga`, `hello/HelloSaga` 세 샘플이 있고 전부 Builder 하나로 짧게 쓴다.

### 사용 패턴 (`core/.../bookingsaga/TripBookingWorkflowImpl.java`)

```java
Saga.Options sagaOptions = new Saga.Options.Builder()
    .setParallelCompensation(true)   // 보상을 병렬 실행
    .build();
Saga saga = new Saga(sagaOptions);
try {
    saga.addCompensation(activities::cancelCar, reqId, name);   // "되돌릴 방법"을 먼저 등록
    String carId = activities.reserveCar(reqId, name);           // 그 다음 실제 호출

    saga.addCompensation(activities::cancelHotel, reqId2, name);
    String hotelId = activities.bookHotel(reqId2, name);

    return new Booking(carId, hotelId, ...);
} catch (ActivityFailure e) {
    // 취소되어도 보상이 끝까지 돌도록 detached scope에서 compensate 호출
    Workflow.newDetachedCancellationScope(() -> saga.compensate()).run();
    throw e;
}
```

### `Saga.Options` 주요 옵션

| 옵션 | 용도 | 기본값 |
| --- | --- | --- |
| `setParallelCompensation(boolean)` | `true`면 `addCompensation`으로 등록된 보상을 **동시에** 실행. `false`면 **등록 역순으로 하나씩** (`core/.../bookingsaga/...`는 true, `core/.../hello/HelloSaga.java`는 false) | `false` |
| `setContinueWithError(boolean)` | 보상 중 하나가 예외를 던져도 **멈추지 않고 나머지 보상을 계속 실행**. `false`면 첫 실패에서 즉시 중단 — 뒤쪽 보상은 유실됨 | `false` |

> **`parallel = false`를 선택해야 하는 경우** — 보상 간 순서 의존이 있을 때 (예: "DB 롤백 → 캐시 무효화"는 역순으로 하나씩). 샘플 `HelloSaga.java`가 명시적으로 `false`를 설정해 "순서 보장"을 보여준다.
>
> **`continueWithError = true`를 선택해야 하는 경우** — 보상이 하나 실패해도 다른 리소스는 반드시 롤백해야 할 때(예: 각각 다른 외부 시스템). 운영에서는 거의 켜두는 쪽이 안전하다.

### 핵심 호출

| 호출 | 설명 |
| --- | --- |
| `saga.addCompensation(Functions.Proc proc, Object... args)` | 보상 함수와 인자 등록. **실제 작업 호출 "직전"** 에 등록해야 함(타임아웃으로 성공 여부가 모호한 상황 대비) |
| `saga.compensate()` | 등록된 보상을 Options에 따라 실행. 보통 `catch` 블록에서 호출 |

### 자주 하는 실수

1. **보상을 작업 "뒤"에 등록** — `reserveCar()`가 서버에 요청은 보냈지만 응답 전에 타임아웃나면 `addCompensation`에 도달하지 못한다. 반드시 **호출 직전** 등록.
2. **`compensate()`를 그냥 `catch`에서 호출** — 워크플로가 외부에서 cancel되면 보상도 함께 취소된다. 반드시 `Workflow.newDetachedCancellationScope(() -> saga.compensate()).run()`으로 감싸야 보상이 끝까지 돈다. (세 샘플 모두 이 패턴을 쓴다.)
3. **보상 함수가 멱등하지 않음** — 작업이 실제로 안 됐을 수도 있으니, 보상은 "없으면 무시"로 설계해야 한다.

---
## 11. `ChildWorkflowOptions` — 자식 워크플로 호출 옵션

### 역할
`Workflow.newChildWorkflowStub(...)` 또는 `Workflow.newUntypedChildWorkflowStub(...)`에 넘기는 옵션. **자식 워크플로 1건**의 ID, TaskQueue, 타임아웃, 부모 종료 시 동작 등을 결정한다. `WorkflowOptions`와 필드가 많이 겹치지만 **자식 워크플로 전용 옵션**(`ParentClosePolicy`, `CancellationType`)이 추가된다.

### 생성
```java
ChildWorkflowOptions opts =
    ChildWorkflowOptions.newBuilder()
        .setWorkflowId("childWorkflow")
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
        .build();

ChildWorkflow child = Workflow.newChildWorkflowStub(ChildWorkflow.class, opts);
Async.function(child::executeChild);                                    // 비동기
Promise<WorkflowExecution> started = Workflow.getWorkflowExecution(child); // 시작 확인
```

### 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setWorkflowId(String)` | 자식 워크플로 비즈니스 ID. 생략 시 서버가 UUID 생성 (`core/.../asyncchild/ParentWorkflowImpl.java`) |
| `setTaskQueue(String)` | 자식을 다른 TaskQueue의 워커에서 돌릴 때. 기본은 부모의 TaskQueue |
| `setParentClosePolicy(ParentClosePolicy)` | **자식 워크플로 전용**. 부모가 종료될 때 자식 처리: `TERMINATE`(기본, 자식도 종료), `ABANDON`(자식 계속 실행), `REQUEST_CANCEL`(자식에 취소 요청) (`core/.../asyncchild/*`, `.../asyncuntypedchild/*`, `.../batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`) |
| `setCancellationType(ChildWorkflowCancellationType)` | 자식 취소 전파 방식: `TRY_CANCEL`, `WAIT_CANCELLATION_REQUESTED`, `WAIT_CANCELLATION_COMPLETED`(기본), `ABANDON` (`core/.../hello/HelloWorkflowTimer.java`) |
| `setWorkflowIdReusePolicy(WorkflowIdReusePolicy)` | WorkflowOptions와 동일 — 같은 자식 ID 재사용 정책 |
| `setWorkflowRunTimeout(Duration)` | 자식 1회 Run 최대 시간 |
| `setWorkflowExecutionTimeout(Duration)` | 자식 Execution 전체 상한 (재시도·cron 포함) |
| `setWorkflowTaskTimeout(Duration)` | 자식의 단일 Workflow Task 처리 시간 |
| `setRetryOptions(RetryOptions)` | 자식 워크플로 레벨 재시도 |
| `setCronSchedule(String)` | 자식 cron 실행 |
| `setSearchAttributes(Map)` / `setTypedSearchAttributes(...)` / `setMemo(Map)` | WorkflowOptions와 동일 — 자식 전용으로 지정 |
| `setStaticSummary(String)` / `setStaticDetails(String)` | 자식 UI 요약·상세 |
| `setVersioningIntent(VersioningIntent)` | Worker Versioning 환경에서 자식을 보낼 버전 |

### `ParentClosePolicy` 심화 — 3가지 동작의 상세 차이

**부모 Workflow Execution이 종료**(완료 / 실패 / cancel / timeout / terminate / **continue-as-new**)되는 **그 순간** 자식 Workflow를 어떻게 처리할지 서버에 지시하는 설정. 코드가 아니라 **서버가 자동으로** 자식에게 조치를 취한다 — 부모 코드가 명시적으로 손 쓸 필요 없음.

#### "부모 종료"의 범위 — continue-as-new 포함

```
부모 종료로 간주되는 상황 전부
├── Workflow Method return (COMPLETED)
├── 예외 throw → Workflow Execution FAILED
├── 외부에서 Workflow Cancel (CANCELED)
├── Timeout (TimedOut)
├── 외부에서 Terminate
└── continue-as-new ← 가장 자주 놓치는 케이스
```

→ `Workflow.continueAsNew(...)`는 **새 Run을 만들고 현재 Run을 끝낸다**. 그래서 ParentClosePolicy가 **작동한다**. 자식이 "같은 Workflow ID 체인에 묶여야 할 것 같은데" 왜 TERMINATE되는지 혼란의 주 원인. §21 참고.

#### 각 값의 동작

**`PARENT_CLOSE_POLICY_TERMINATE` (기본값)**
- 부모 종료 **즉시** 자식에게 **TerminateWorkflowExecution** 요청. 자식은 `TERMINATED` 상태로 종료.
- Terminate는 **cancel과 다르게** 자식 코드가 `CanceledFailure`를 받지 못함 — 바로 끝남. **cleanup 안 됨**.
- Activity나 Child를 쥐고 있어도 서버가 전부 정리.
- **쓸 때** — 자식이 부모 결과의 일부로만 의미 있는 경우 (예: 부모가 집계 결과 반환, 자식은 그 집계용 서브태스크).

**`PARENT_CLOSE_POLICY_ABANDON`**
- 부모 종료 시 자식에게 **아무 조치도 안 함**. 자식은 독립적으로 자기 수명대로 돌아감.
- 자식이 보기엔 부모가 있었는지도 모르는 상태로 계속 실행.
- 자식의 "parent 참조"(`Workflow.getInfo().getParentWorkflowExecution()`)는 **유지**되지만 통신은 끊어짐.
- **쓸 때** — fan-out / batch 패턴에서 **continue-as-new하는 디스패처**가 자식을 쭉 실행하게 할 때. 샘플: `core/.../batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`, `.../asyncchild/ParentWorkflowImpl.java`.

**`PARENT_CLOSE_POLICY_REQUEST_CANCEL`**
- 부모 종료 시 자식에게 **cancel 요청**을 보냄 (Terminate와 다름).
- 자식 코드는 `CanceledFailure`를 받아 **cleanup/보상 로직** 실행 기회를 얻음. §11의 `ChildWorkflowCancellationType`과는 **방향이 반대**(여기선 부모→자식 신호를 서버가 자동 전송).
- 자식이 cleanup을 끝낼 때까지 부모는 **기다리지 않음** — 부모는 이미 끝났고, 자식은 자기 수명 내에서 끝까지 cleanup.
- **쓸 때** — 자식이 외부 리소스를 쥐고 있어 **cleanup 보장은 필요**하지만, 부모가 자식 종료까지 기다릴 필요는 없을 때.

#### 3가지 선택 비교표

| 항목 | **TERMINATE** (기본) | **ABANDON** | **REQUEST_CANCEL** |
| --- | --- | --- | --- |
| 서버가 자식에 보내는 조치 | **TerminateWorkflowExecution** | 없음 | **RequestCancelWorkflowExecution** |
| 자식 코드가 cleanup 실행? | **X** (강제 종료) | 영향 없음 (계속 실행) | **O** (`CanceledFailure` 받음) |
| 자식이 쥔 Activity/손자 처리? | 서버가 전부 중단 | 자식 자신이 관리 | 자식 코드가 cleanup 흐름대로 처리 |
| 자식 최종 상태 | TERMINATED | 자기 수명대로 (COMPLETED/FAILED/CANCELED) | CANCELED (cleanup 성공) 또는 FAILED |
| continue-as-new 안전? | **X** — 자식 전부 죽음 | **O** — 자식 독립 실행 | △ — 자식 cancel되지만 cleanup 돌 수 있음 |
| 부모 종료 속도 영향 | 즉시 | 즉시 | 즉시 (자식 cleanup 기다리지 않음) |

#### `ParentClosePolicy` vs `ChildWorkflowCancellationType` — 혼동 금지

| 개념 | 트리거 | 결정 대상 | 신호 방향 |
| --- | --- | --- | --- |
| **`ParentClosePolicy`** | 부모 **종료**(완료/cancel/CAN 등) | 종료 시 **자식에게 무엇을 할지** | **부모 종료 → 자식** (서버 자동) |
| **`ChildWorkflowCancellationType`** | 부모 코드가 **명시적 cancel** 호출 | 부모가 자식 cancel을 **얼마나 기다릴지** | **부모 cancel 호출 → 부모가 블록 수준 선택** |

→ **cancel 흐름에선 둘 다 작동 가능**. 예: 부모가 cancel되고 `ParentClosePolicy=REQUEST_CANCEL`이면 자식에 cancel 신호가 가고, 자식 stub의 `CancellationType=WAIT_CANCELLATION_COMPLETED`가 걸려 있다면 **부모 종료 전에** 자식 완료까지 기다림.

#### 동기 호출 vs 비동기 호출에서의 의미

| 호출 패턴 | ParentClosePolicy 영향 |
| --- | --- |
| **동기 호출** (`child.execChild(...)`) | 부모가 자식 결과를 기다리므로 **ParentClosePolicy 거의 영향 없음** (자식이 끝나야 부모가 return) |
| **비동기 호출** (`Async.function(child::...)`로 결과 안 기다림) | ParentClosePolicy가 **진짜 영향** — 부모가 자식 결과 없이 끝나면 자식은 어떻게 되나? |
| **비동기 + `Workflow.getWorkflowExecution(child).get()`** (시작만 확인) | 비동기와 동일. 자식은 계속 돌음, ParentClosePolicy에 따라 처리됨 |

→ ParentClosePolicy를 **의미 있게** 쓰려면 거의 항상 **비동기 자식**과 조합. 저장소의 `asyncchild`, `asyncuntypedchild`, `batch/slidingwindow` 샘플이 전부 이 조합.

#### 샘플 유즈케이스 매트릭스

| 상황 | 선택 | 샘플 |
| --- | --- | --- |
| 자식이 부모 집계의 일부 서브태스크 | `TERMINATE` (기본) | — (기본 동작) |
| **continue-as-new 디스패처가 fan-out한 자식들** | `ABANDON` | `core/.../batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java` |
| 부모가 끝나도 자식이 외부 처리 완료해야 함 | `ABANDON` | `core/.../asyncchild/ParentWorkflowImpl.java`, `.../asyncuntypedchild/ParentWorkflowImpl.java` |
| 자식이 쥔 외부 리소스는 **반드시 cleanup**, 부모는 기다리지 않음 | `REQUEST_CANCEL` | (저장소 샘플 없음, 패턴 자체는 유효) |

#### 자주 하는 실수

1. **`continue-as-new` + 기본 `TERMINATE`** — 가장 자주 보는 버그. "ID가 같은 Workflow 체인에서 자식은 왜 사라지지?"의 원인. 명시적 `ABANDON` 필수. §21의 체크리스트에도 명시.
2. **`REQUEST_CANCEL`인데 자식이 cleanup을 안 함** — 자식 코드가 `CanceledFailure`를 catch하지 않거나 `Workflow.newDetachedCancellationScope`로 보호 안 하면 cleanup 유실. §14 자주 하는 실수 참고.
3. **동기 자식에 ParentClosePolicy 설정하고 의미 없음** — 동기 호출은 자식 완료까지 부모가 기다리므로 영향 없음. 설정 자체는 유효하지만 혼란만 가중.
4. **`ABANDON`인데 자식 Workflow ID가 부모 ID 기반** — 부모가 continue-as-new 후 **다음 Run이 같은 자식 ID로 새 자식**을 띄우면 `WorkflowIdReusePolicy` 충돌 가능. 자식 ID 생성 전략(부모 Run ID 포함 등) 신중히.
5. **`TERMINATE`인데 자식이 돈 송금 중** — 자식이 외부 트랜잭션 중간에 강제 종료되면 상태 꼬임. 비즈니스 임계 작업은 `ABANDON` 또는 `REQUEST_CANCEL`로 보호.
6. **Terminate와 Cancel 혼동** — Terminate는 자식이 아무 cleanup 못 하고 바로 끝남. Cancel은 `CanceledFailure` 받고 cleanup 기회. "`TERMINATE`로 설정하면 cleanup 돌 거야"는 **틀림**.

### `ChildWorkflowCancellationType` 심화 — 4가지 동작의 상세 차이

Activity와 비슷하지만 **값이 4개**로 Activity(3개)보다 하나 더 세밀하다. 자식 워크플로는 **별도 Workflow Execution**이므로 "cancel 요청이 자식에게 도착했는가"와 "자식이 cleanup을 끝냈는가"를 **구분해서** 대기할 수 있다. 샘플 사용 예: `core/.../hello/HelloWorkflowTimer.java`.

#### cancel이 자식에 전달되는 4단계 흐름

```
T0 (부모가 scope.cancel 또는 childFuture.cancel 호출)
 │
 ▼
① 부모가 서버에 "RequestCancelExternalWorkflowExecution" 커맨드 전송
 │
 ▼
② 서버가 자식 Workflow에 WorkflowExecutionCancelRequested 이벤트 적재
 │   = "자식이 cancel 요청을 수신"
 ▼
③ 자식 Workflow가 다음 Workflow Task 처리 시 CanceledFailure 흐름을 타기 시작
 │   (안쪽 Activity cancel, cleanup 수행 등)
 ▼
④ 자식 Workflow가 종료 (CANCELED 상태로 완료)
```

**`ChildWorkflowCancellationType`은 부모가 이 4단계 중 어느 지점까지 블록할지**를 결정.

#### 각 값의 동작

**`TRY_CANCEL`**
- ① **cancel 커맨드만 발행**하고 부모는 즉시 `CanceledFailure`를 받고 다음 코드로 진행.
- 자식 Workflow에 cancel이 도착했는지 **확인하지 않음**.
- **쓸 때** — "자식을 끊고 싶지만 자식이 실제로 멈췄는지는 신경 안 씀". 가장 빠름.

**`WAIT_CANCELLATION_REQUESTED`** ← Activity엔 없는 값
- ② **자식이 cancel 요청을 수신 확인**할 때까지만 대기. 그 뒤 부모는 `CanceledFailure`를 받고 다음 코드로 진행.
- 자식이 **cleanup을 다 끝냈는지는 안 기다림**.
- **쓸 때** — "자식에게 cancel이 분명히 전달됐다는 보증은 필요하지만, 자식 내부 cleanup 완료까지 기다리면 너무 느림". TRY_CANCEL보다 안전하고 WAIT_CANCELLATION_COMPLETED보다 빠른 중간값.

**`WAIT_CANCELLATION_COMPLETED` (기본값)**
- ④ **자식 Workflow가 완전히 종료**(CANCELED/FAILED/COMPLETED)될 때까지 부모 블록.
- 자식 내부 Activity cleanup, 보상 로직, detached scope cleanup 전부 반영된 최종 상태를 받음.
- `ChildWorkflowFailure`의 `getCause()`에 `CanceledFailure` 또는 자식이 던진 다른 예외가 담김.
- **쓸 때** — "자식의 cleanup 결과가 부모 다음 로직의 전제". 예: 자식이 리소스 반환을 끝내야 부모가 다음 리소스 할당 진행.

**`ABANDON`**
- cancel 커맨드 자체를 서버에 보내지 않음. 자식은 자기가 cancel 당한 걸 **모른 채** 평소대로 실행.
- 부모는 즉시 `CanceledFailure`를 받고 다음 코드로 진행.
- 자식의 최종 결과는 부모에게 전달되지 않음 (버려짐).
- **쓸 때** — "이미 시작된 자식을 중간에 끊으면 안 되는 종류의 작업". 자식이 긴 트랜잭션을 돌고 있고 중단 시 상태가 꼬이는 경우.

#### 4가지 선택 비교표

| 항목 | **TRY_CANCEL** | **WAIT_CANCELLATION_REQUESTED** | **WAIT_CANCELLATION_COMPLETED** (기본) | **ABANDON** |
| --- | --- | --- | --- | --- |
| 부모가 `CanceledFailure` 받는 시점 | **즉시** (커맨드 발행 직후) | 자식이 **cancel 수신 확인** 후 | 자식이 **완전히 종료**된 후 | **즉시** |
| 서버에 cancel 커맨드 발행? | O | O | O | **X** |
| 자식 workflow가 cancel을 **알아챔**? | O (타이밍은 보증 X) | **O (보증됨)** | O | **X** |
| 자식 cleanup/보상 **완료 보증**? | X | X | **O** | 해당 없음 |
| 자식 최종 결과 전달? | X | X | **O** (`ChildWorkflowFailure`) | X |
| 부모 다음 코드 진행 속도 | 가장 빠름 | 중간 | 가장 느림 (자식 수명에 종속) | 가장 빠름 |
| 자식이 장시간 돌면 부모 영향? | 없음 | 작음 (수신 확인만 기다림) | **큼** (자식 완료까지 블록) | 없음 |

#### Activity와의 비교

| 항목 | **ActivityCancellationType** | **ChildWorkflowCancellationType** |
| --- | --- | --- |
| 값 개수 | 3 (`TRY_CANCEL` / `WAIT_CANCELLATION_COMPLETED` / `ABANDON`) | **4** (추가로 `WAIT_CANCELLATION_REQUESTED`) |
| 기본값 | **`TRY_CANCEL`** | **`WAIT_CANCELLATION_COMPLETED`** ← 반대 |
| cancel 수신 조건 | Activity가 **heartbeat** 호출 필요 | 자식 Workflow가 다음 **Workflow Task** 처리 (heartbeat 불필요) |
| "cleanup 완료 보장" 수단 | WAIT_CANCELLATION_COMPLETED (heartbeat 필수) | WAIT_CANCELLATION_COMPLETED (heartbeat 불필요, 자식이 알아서 종료 처리) |

> **기본값이 다른 이유** — Activity는 "짧은 작업"으로 가정해 빠른 TRY_CANCEL이 기본. Child Workflow는 "하위 비즈니스 로직 단위"로 자식 완료까지 기다리는 게 안전한 기본.

#### 부모 쪽 catch 패턴 (`HelloWorkflowTimer.java`)

```java
WorkflowWithTimerChildWorkflow child = Workflow.newChildWorkflowStub(
    WorkflowWithTimerChildWorkflow.class,
    ChildWorkflowOptions.newBuilder()
        .setWorkflowId(WORKFLOW_ID + "-Child")
        .setCancellationType(ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED)
        .build());

try {
    child.executeChild(input);                           // 블로킹 호출
} catch (ChildWorkflowFailure cwf) {                     // ← Activity와 달리 ChildWorkflowFailure
    if (cwf.getCause() instanceof CanceledFailure) {
        workflowResult = "Timer fired while child was executing.";
    }
}
```
`ActivityFailure` → `CanceledFailure`가 Activity 패턴이라면, Child에서는 **`ChildWorkflowFailure` → `getCause()` → `CanceledFailure`**.

#### `ParentClosePolicy`와 혼동 금지

| 개념 | 트리거 | 결정하는 것 |
| --- | --- | --- |
| **`ChildWorkflowCancellationType`** | 부모가 **명시적으로 cancel** 호출 | 부모가 자식 cancel을 **얼마나 기다릴지** |
| **`ParentClosePolicy`** | 부모 Workflow가 **종료**(완료/실패/cancel/continue-as-new) | 종료 시 자식에게 **무엇을 할지** (TERMINATE / ABANDON / REQUEST_CANCEL) |

→ cancel 흐름에서 둘 다 작동 가능. 예: 부모가 cancel되면 `ParentClosePolicy=REQUEST_CANCEL`로 자식에 cancel 요청이 가고, **동시에** 자식 stub의 `CancellationType=WAIT_CANCELLATION_COMPLETED`가 걸려 있다면 부모는 자식 종료까지 기다리고 종료.

#### 상황별 선택 가이드

| 상황 | 선택 |
| --- | --- |
| 레이싱 패턴에서 지는 자식 버리기 | **TRY_CANCEL** |
| 자식에게 cancel이 "접수됐다"는 보증만 필요 | **WAIT_CANCELLATION_REQUESTED** |
| 자식 cleanup 결과를 보고 부모가 분기 | **WAIT_CANCELLATION_COMPLETED** (기본) |
| 자식이 외부 트랜잭션 중, 중단 시 상태 꼬임 | **ABANDON** + `ParentClosePolicy.ABANDON` |
| 자식을 cancel해도 자식이 cleanup으로 몇 분 걸림, 부모는 빨리 끝내야 함 | **TRY_CANCEL** 또는 **WAIT_CANCELLATION_REQUESTED** |
| 자식이 수많은 손자(grandchild)를 가져 cleanup이 긴 체인 | **WAIT_CANCELLATION_REQUESTED**로 "분명 전달됐다"만 확인하고 분리 |

#### Nexus와의 비교 (참고)

Nexus의 `NexusOperationCancellationType`도 유사한 4값 체계지만 **이름이 짧고**(`WAIT_CANCELLATION_` 접두사 없이 `WAIT_REQUESTED` / `WAIT_COMPLETED`), **네트워크 홉이 하나 더** 걸리는 만큼 기본값(`WAIT_COMPLETED`)이 더 보수적. 상세는 §13의 "`NexusOperationCancellationType` 심화" 참고.

---

### 자식 워크플로 호출 패턴

```java
// 1) 동기 — 자식 종료까지 대기
String result = child.execChild(name, title);
// → core/.../countinterceptor/workflow/MyWorkflowImpl.java

// 2) 비동기 — 자식 시작/결과를 Promise로
Async.function(child::executeChild);
Promise<WorkflowExecution> started = Workflow.getWorkflowExecution(child);
// → core/.../asyncchild/ParentWorkflowImpl.java

// 3) Untyped — 타입을 몰라도 됨 (동적 자식 선택)
ChildWorkflowStub untyped = Workflow.newUntypedChildWorkflowStub("ChildType", opts);
untyped.executeAsync(String.class, "Hello", name);
// → core/.../asyncuntypedchild/ParentWorkflowImpl.java
```

### `WorkflowOptions` vs `ChildWorkflowOptions`

- **스타터(클라이언트 측)에서 워크플로 시작** → `WorkflowOptions`
- **워크플로 코드 안에서 자식 워크플로 시작** → `ChildWorkflowOptions`
- 공통 필드(ID, TaskQueue, 타임아웃, SearchAttributes 등)의 의미는 동일.
- `ChildWorkflowOptions`에만 있는 것: `ParentClosePolicy`, `CancellationType`.
- `WorkflowOptions`에만 있는 것: `StartDelay`, `DisableEagerExecution` (스타터에서 시작할 때만 의미 있음).

---
## 13. `NexusServiceOptions` / `NexusOperationOptions` — Nexus 서비스 호출 옵션

### 역할
**Nexus**는 Temporal의 cross-namespace / cross-service 호출 메커니즘. 다른 Temporal 서비스의 워크플로나 외부 서비스를 **Workflow처럼 네트워크 안전하게** 호출할 수 있게 해준다.

- `NexusServiceOptions` — Nexus **서비스 전체** 수준 설정 (어느 endpoint로 보낼지)
- `NexusOperationOptions` — 그 서비스 안의 **각 operation 호출** 수준 설정 (타임아웃·취소)

둘은 중첩 구조로 함께 사용된다.

### 생성 (`core/.../nexuscancellation/caller/HelloCallerWorkflowImpl.java`)

```java
SampleNexusService service =
    Workflow.newNexusServiceStub(
        SampleNexusService.class,
        NexusServiceOptions.newBuilder()
            .setOperationOptions(
                NexusOperationOptions.newBuilder()
                    .setScheduleToCloseTimeout(Duration.ofSeconds(10))
                    .setCancellationType(NexusOperationCancellationType.WAIT_REQUESTED)
                    .build())
            .build());
```

### 두 가지 설정 지점

Nexus 옵션은 **워크플로 코드 안** 또는 **워커 등록 시** 두 지점에서 지정할 수 있다. 등록 시 설정은 코드 안에서 매번 반복할 필요를 없애고 운영/배포 쪽에서 바꿀 수 있게 한다.

```java
// 1) 워크플로 코드 안 (호출 시마다)
Workflow.newNexusServiceStub(SampleNexusService.class, serviceOptions);
// → core/.../nexuscancellation/caller/HelloCallerWorkflowImpl.java
// → core/.../nexusmultipleargs/caller/HelloCallerWorkflowImpl.java

// 2) 워커 등록 시 (WorkflowImplementationOptions.setNexusServiceOptions)
worker.registerWorkflowImplementationTypes(
    WorkflowImplementationOptions.newBuilder()
        .setNexusServiceOptions(Map.of(
            SampleNexusService.class.getSimpleName(),
            NexusServiceOptions.newBuilder().setEndpoint("my-nexus-endpoint-name").build()))
        .build(),
    HelloCallerWorkflowImpl.class);
// → core/.../nexuscancellation/caller/CallerWorker.java
// → core/.../nexus/caller/CallerWorkflowTest.java (테스트에서 endpoint 바꿔치기)
```

### `NexusServiceOptions` 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setEndpoint(String)` | **필수**. 호출할 Nexus endpoint 이름 (서버에 등록된 이름). 테스트/운영 환경에 따라 바뀌므로 등록 시 지정 패턴이 유용 (`core/.../nexus/caller/CallerWorkflowTest.java`) |
| `setOperationOptions(NexusOperationOptions)` | 이 서비스의 **모든 operation에 적용**되는 기본 옵션 |
| `setOperationMethodOptions(Map<String, NexusOperationOptions>)` | **메서드명 → 옵션** 매핑. operation 별로 다르게 설정하고 싶을 때 |

### `NexusOperationOptions` 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setScheduleToCloseTimeout(Duration)` | **operation 전체 상한** (재시도 포함). Nexus는 서버가 재시도를 관리하므로 이 값이 사실상 유일한 "포기 시점" (`core/.../nexuscancellation/...`, `core/.../nexusmultipleargs/...`) |
| `setCancellationType(NexusOperationCancellationType)` | 취소 전파 방식: `WAIT_COMPLETED`(기본, operation 완료까지 대기), `WAIT_REQUESTED`(handler가 취소 요청 수신까지만 대기), `TRY_CANCEL`(요청만 보내고 즉시 반환), `ABANDON`(요청 안 보냄) (`core/.../nexuscancellation/caller/HelloCallerWorkflowImpl.java`) |
| `setSummary(String)` | UI 표시용 요약 (1.24+) |

### 호출 패턴

```java
// 동기 호출 — operation 완료까지 블록
Result r = sampleNexusService.hello(input);

// 비동기 — Promise 반환
Promise<Result> p = Async.function(sampleNexusService::hello, input);

// CancellationScope와 함께 "먼저 끝나는 하나만 쓰고 나머지 취소"
CancellationScope scope = Workflow.newCancellationScope(() -> {
    for (var lang : languages) results.add(Async.function(service::hello, new Input(msg, lang)));
});
scope.run();
Result first = Promise.anyOf(results).get();
scope.cancel();   // 나머지 operation은 CancellationType 설정대로 처리됨
// → core/.../nexuscancellation/caller/HelloCallerWorkflowImpl.java
```

### `NexusOperationCancellationType` 심화 — 4가지 동작의 상세 차이

Nexus는 **서비스/namespace 경계를 넘는 호출**이라 cancel 전파가 여러 네트워크 홉을 거친다. 그래서 Child Workflow와 비슷한 **4값 체계**지만 기본값과 세부 의미가 다르다.

#### cancel이 handler까지 전파되는 흐름

```
T0 (caller workflow가 scope.cancel 또는 opPromise.cancel 호출)
 │
 ▼
① caller 서버에 "RequestCancelNexusOperation" 커맨드 발행
 │
 ▼ (네트워크 홉: caller 서버 → endpoint 서버)
② endpoint 서버가 Nexus handler에게 CancelOperation 요청 전달
 │   = "handler가 cancel 요청을 수신 확인"
 ▼
③ handler Workflow가 다음 Task에서 CanceledFailure 흐름을 타기 시작
 │   (내부 Activity/보상/cleanup 수행)
 ▼
④ handler operation이 종료 (CANCELED/FAILED/COMPLETED 상태)
```

→ **단일 서비스 안의 Child보다 네트워크 홉이 1~2개 더 많아**, "요청 보냄 ↔ 실제 handler가 받음" 사이 지연이 크고 실패 가능성도 높다.

#### 각 값의 동작

**`NexusOperationCancellationType.TRY_CANCEL`**
- ① caller 서버에 **cancel 커맨드만 발행**하고 caller workflow는 즉시 `CanceledFailure` 받고 다음 코드로.
- handler까지 요청이 **도달했는지 확인하지 않음**.
- 네트워크 홉 중간에 요청이 유실될 수도 있음 (서버가 재시도는 하지만 caller는 모름).
- **쓸 때** — "endpoint 서버에만 "취소하라" 신호를 던지고 나머지는 신경 안 씀". 가장 빠름.

**`NexusOperationCancellationType.WAIT_REQUESTED`**
- ② **handler가 cancel 요청을 수신 확인**(ack)할 때까지 대기한 뒤 caller가 `CanceledFailure` 받음.
- handler의 cleanup 수행 완료는 안 기다림.
- 네트워크 홉 도달 보증은 받되, operation 완료 지연 비용은 피함.
- **쓸 때** — "handler까지 cancel이 분명히 전달됐다"는 보증은 필요하지만 "handler 내부 cleanup까지 기다리면 네트워크 왕복이 길어져 느림". `core/.../nexuscancellation/caller/HelloCallerWorkflowImpl.java`가 사용하는 값.

**`NexusOperationCancellationType.WAIT_COMPLETED` (기본값)**
- ④ **handler operation이 완전히 종료**될 때까지 caller 블록.
- handler 내부 `Workflow.sleep`, Activity cleanup, 보상 로직까지 전부 반영된 최종 상태 수신.
- `NexusOperationFailure`의 `getCause()`에 `CanceledFailure` 또는 handler가 던진 다른 예외.
- **쓸 때** — "handler의 cleanup 완료가 caller 다음 로직의 전제" (예: 외부 서비스에 리소스 반환이 실제로 끝나야 다음 할당).

**`NexusOperationCancellationType.ABANDON`**
- cancel 커맨드 자체를 서버에 보내지 않음. handler는 자기가 cancel 당한 걸 **모른 채** 평소대로 실행.
- caller는 즉시 `CanceledFailure` 받고 다음 코드로.
- handler 최종 결과는 caller에 전달되지 않음 (버려짐).
- **쓸 때** — "외부 서비스에 비즈니스적으로 커밋된 요청이라 중간 중단이 재앙".

#### 4가지 선택 비교표

| 항목 | **TRY_CANCEL** | **WAIT_REQUESTED** | **WAIT_COMPLETED** (기본) | **ABANDON** |
| --- | --- | --- | --- | --- |
| caller가 `CanceledFailure` 받는 시점 | **즉시** | handler **ack 수신** 후 | handler **완전 종료** 후 | **즉시** |
| 서버에 cancel 커맨드 발행? | O | O | O | **X** |
| handler까지 **cancel 도달 보증**? | X (유실 가능) | **O** | O | X |
| handler cleanup/보상 **완료 보증**? | X | X | **O** | 해당 없음 |
| handler 최종 결과 전달? | X | X | **O** (`NexusOperationFailure`) | X |
| 네트워크 홉 비용 | 최소 | 중간 (왕복 1회) | 최대 (operation 수명만큼) | 없음 |
| caller가 외부 서비스 지연에 종속? | X | 작음 | **큼** | X |

#### handler 쪽 코드 — cancel 후 cleanup (`core/.../nexuscancellation/handler/HelloHandlerWorkflowImpl.java`)

```java
@Override
public SampleNexusService.HelloOutput hello(SampleNexusService.HelloInput input) {
    try {
        Workflow.sleep(Duration.ofSeconds(Workflow.newRandom().nextInt(5)));
        // ... 비즈니스 로직
        return new SampleNexusService.HelloOutput("Hello " + input.getName());
    } catch (CanceledFailure e) {
        // ⚠️ detached scope로 감싸야 cleanup이 cancel에 휩쓸리지 않음 (§14 패턴 3)
        Workflow.newDetachedCancellationScope(
                () -> Workflow.sleep(Duration.ofSeconds(Workflow.newRandom().nextInt(5))))
            .run();
        log.info("HelloHandlerWorkflow was cancelled successfully.");
        throw e;                        // 반드시 re-throw
    }
}
```
→ handler가 `CanceledFailure` catch 후 cleanup 하려면 **detached scope 필수**. 그냥 `Workflow.sleep` 호출하면 scope가 이미 취소되어 즉시 터짐.

#### caller 쪽 catch 패턴 (`HelloCallerWorkflowImpl.java`)

```java
Promise<HelloOutput> p = Async.function(service::hello, input);
...
scope.cancel();
try {
    p.get();
} catch (NexusOperationFailure e) {     // ← Activity도 Child도 아닌 NexusOperationFailure
    if (e.getCause() instanceof CanceledFailure) {
        log.info("Operation was cancelled");
        continue;                        // 다음 operation으로
    }
    throw e;
}
```
세 가지 수준의 Failure 타입이 다름 — **Activity → `ActivityFailure`**, **Child → `ChildWorkflowFailure`**, **Nexus → `NexusOperationFailure`**. 전부 `getCause()`로 `CanceledFailure` 확인.

#### Activity / Child / Nexus — 3가지 Cancellation Type 통합 비교

| 항목 | **ActivityCancellationType** | **ChildWorkflowCancellationType** | **NexusOperationCancellationType** |
| --- | --- | --- | --- |
| 값 개수 | 3 | 4 | **4** |
| 기본값 | **`TRY_CANCEL`** | **`WAIT_CANCELLATION_COMPLETED`** | **`WAIT_COMPLETED`** |
| "ack 수신까지만 대기" 값 | 없음 | `WAIT_CANCELLATION_REQUESTED` | **`WAIT_REQUESTED`** |
| cancel 수신 조건 | Activity의 **heartbeat** | 자식의 다음 **Workflow Task** | handler의 다음 **Workflow Task** + 네트워크 홉 |
| 실패 wrapper 타입 | `ActivityFailure` | `ChildWorkflowFailure` | **`NexusOperationFailure`** |
| 네트워크 홉 왕복 | caller ↔ caller 서버 | caller ↔ caller 서버 | caller 서버 ↔ **endpoint 서버** ↔ handler |
| "기본값이 보수적인 이유" | 짧은 작업 전제 → 빠른 TRY_CANCEL | 하위 로직 단위 → 완료까지 대기 안전 | 네트워크 왕복 + 외부 서비스 신뢰성 → 가장 보수적 |

> **왜 Nexus가 가장 보수적인가** — Nexus는 **다른 서비스/namespace**를 호출. 서비스 경계 너머의 상태를 "모른 채" 다음 로직을 진행하면 분산 시스템 일관성이 깨지기 쉽다. 기본값으로 **완료 보증**이 안전.

#### 상황별 선택 가이드

| 상황 | 선택 |
| --- | --- |
| 레이싱 패턴에서 지는 operation 버리기 | **TRY_CANCEL** 또는 **WAIT_REQUESTED** (ack만 확보) |
| handler가 cleanup을 수행하는데 완료는 안 기다려도 됨 | **WAIT_REQUESTED** (`nexuscancellation` 샘플 스타일) |
| handler cleanup 결과가 caller 다음 로직 전제 | **WAIT_COMPLETED** (기본) |
| 외부 서비스에 돈이 송금된 operation 중단은 재앙 | **ABANDON** |
| handler Workflow가 길어 caller가 블록되면 UX 저하 | **WAIT_REQUESTED** |
| caller와 handler가 서로 다른 팀 소유, SLA 격리 중요 | **WAIT_REQUESTED** 또는 **TRY_CANCEL** |

#### 운영 포인트

- **`setScheduleToCloseTimeout`과 함께 설정** — WAIT_COMPLETED는 handler 완료까지 블록하므로, handler가 멈춰 있으면 caller도 멈춤. 반드시 ScheduleToCloseTimeout으로 상한을 걸어둔다.
- **detached scope와 조합** — handler 쪽에서 cleanup 수행 시 반드시 `Workflow.newDetachedCancellationScope(...)` (위 샘플). 안 그러면 cancel이 cleanup까지 전파돼 cleanup도 못 돌고 터짐.
- **네트워크 분단 상황** — WAIT_REQUESTED/WAIT_COMPLETED는 endpoint 서버와의 통신이 복구될 때까지 대기. caller 쪽 ScheduleToCloseTimeout이 사실상 안전망.

---

### `ChildWorkflow`와의 차이

| 항목 | ChildWorkflow | Nexus Operation |
| --- | --- | --- |
| 호출 범위 | 같은 namespace 안 | **다른 namespace / 다른 서비스**도 가능 |
| 호출 지점 식별 | 자식 Workflow Type | Endpoint + Service + Operation |
| 커플링 | 코드/배포 공유 | 서비스 경계로 분리 (팀 간 계약) |
| 사용 시점 | 같은 서비스 내부 분해 | 서비스 간/네임스페이스 간 호출 |

### 샘플 레퍼런스
- 서비스 호출 + Cancellation 조합: `core/src/main/java/io/temporal/samples/nexuscancellation/caller/HelloCallerWorkflowImpl.java`
- 다중 인자 operation: `core/src/main/java/io/temporal/samples/nexusmultipleargs/caller/HelloCallerWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/nexusmultipleargs/caller/EchoCallerWorkflowImpl.java`
- 워커 등록 시 endpoint 주입: `core/src/main/java/io/temporal/samples/nexuscancellation/caller/CallerWorker.java`, `core/src/main/java/io/temporal/samples/nexusmultipleargs/caller/CallerWorker.java`
- 테스트에서 endpoint 바꿔치기: `core/src/test/java/io/temporal/samples/nexus/caller/CallerWorkflowTest.java`
