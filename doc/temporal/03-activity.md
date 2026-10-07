# Activity 옵션 — Options / Retry / 구현 등록 / Local

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 7. `ActivityOptions` — Activity 호출 당 타임아웃/재시도

### 역할
워크플로 코드 안에서 `Workflow.newActivityStub(...)`에 넘기는 옵션. **각 Activity 호출**(ScheduleActivity 커맨드) 수준의 타임아웃·재시도·태스크큐·취소 동작을 결정한다. Activity 타입별로 다르게 주고 싶으면 `WorkflowImplementationOptions.setActivityOptions(Map)`을 쓴다(`core/.../peractivityoptions/Starter.java`).

### 생성
```java
private final GreetingActivities activities =
    Workflow.newActivityStub(
        GreetingActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(2))
            .build());
```

### 네 가지 타임아웃 (가장 중요)

Temporal Activity는 수명 주기에서 네 타이머가 돈다. 셋 중 어느 하나는 **반드시** 설정해야 서버가 받아준다 — 일반적으로 `StartToCloseTimeout`.

```
스케줄 ──[ScheduleToStart]── 워커 수신 ──[StartToClose]── 완료
   └──────────[ScheduleToClose: 전체 상한]──────────┘
                              ↑
                   [Heartbeat: 진행중 ping 간격]
```

| 옵션 | 용도 |
| --- | --- |
| `setStartToCloseTimeout(Duration)` | **1회 시도** 최대 실행 시간. 거의 항상 설정. 초과 시 재시도 트리거 |
| `setScheduleToCloseTimeout(Duration)` | 재시도 포함 Activity **전체** 상한 (`core/.../peractivityoptions/Starter.java`) |
| `setScheduleToStartTimeout(Duration)` | 큐에 들어간 뒤 워커가 집어들기까지 상한. 포화 감지용 (`core/.../hello/HelloException.java`, `.../fileprocessing/FileProcessingWorkflowImpl.java`) |
| `setHeartbeatTimeout(Duration)` | long-running Activity의 heartbeat 간격 상한. 넘기면 워커가 죽었다고 판단 → 재시도 (`core/.../hello/HelloWorkflowTimer.java`) |

### 그 외 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setRetryOptions(RetryOptions)` | 재시도 정책. `MaximumAttempts`, `InitialInterval`, `BackoffCoefficient`, `MaximumInterval`, `DoNotRetry(classNames…)` (`springai/rag/.../RagWorkflowImpl.java`, `core/.../peractivityoptions/Starter.java`) |
| `setTaskQueue(String)` | 이 Activity만 다른 TaskQueue의 워커에 보내고 싶을 때. 기본은 워크플로의 TaskQueue |
| `setCancellationType(ActivityCancellationType)` | 취소 전파 방식: `TRY_CANCEL`(기본, 즉시 반환), `WAIT_CANCELLATION_COMPLETED`(Activity가 취소를 confirm할 때까지 대기), `ABANDON`(취소 신호 안 보냄) (`core/.../hello/HelloWorkflowTimer.java`) |
| `setDisableEagerExecution(boolean)` | Eager Activity 비활성화. 로컬 워커 바로 실행 대신 일반 폴링 경로 |
| `setHeartbeatDetails(Object)` | — Builder가 아닌 `Activity.getExecutionContext().heartbeat(details)`에서 사용 (여기선 참고용) |
| `setContextPropagators(List<ContextPropagator>)` | 이 Activity 호출에만 다른 컨텍스트 전파기를 사용 |
| `setVersioningIntent(VersioningIntent)` | Worker Versioning 환경에서 "같은 버전/기본 버전 중 어디로 보낼지" 지정 |
| `setSummary(String)` | UI 표시용 요약 (1.24+) |

### `ActivityCancellationType` 심화 — 3가지 동작의 상세 차이

cancel 신호는 **협력적(cooperative)**이다 (§14 참고). "Workflow가 scope.cancel 또는 Workflow cancel을 호출" → 서버가 Activity에 cancel 요청 → Activity 코드가 **heartbeat 호출 시점에** `ActivityCompletionException`을 받음. `ActivityCancellationType`은 이 3단계에서 **Workflow가 어느 지점까지 대기할지**를 결정한다.

#### 수명 주기 비교

```
                    T0 (Workflow가 scope.cancel 호출)
                    │
    ┌───────────────┼───────────────────────────────────┐
    │               │                                   │
    ▼ (TRY_CANCEL)  ▼ (WAIT_CANCELLATION_COMPLETED)     ▼ (ABANDON)
Workflow 즉시 CanceledFailure    서버가 cancel 요청        아무 요청도 서버에
받음. Activity는 다음 heartbeat ─── Activity가 heartbeat   보내지 않음.
때 알게 됨 (늦게)                    에서 알아채고          Activity는 자기
                                     cleanup 끝낼 때까지    할 일 다 하고
                                     Workflow가 기다림      결과를 Workflow에
                                                           돌려줌
```

#### 각 값의 동작

**`ActivityCancellationType.TRY_CANCEL` (기본값)**
- Workflow가 즉시 **`CanceledFailure`가 cause인 `ActivityFailure`** 를 받고 다음 코드로 진행.
- 서버는 Activity에게 cancel 요청을 보내지만, **Activity의 응답을 기다리지 않음**.
- Activity 쪽은 **heartbeat 호출 시점에** `ActivityCompletionException`을 throw받아 알게 됨 → cleanup 수행.
- Activity의 cleanup 결과(완료/실패)는 Workflow에 전달되지 않음 (이미 Workflow가 다음 코드로 이동했으므로).
- **쓸 때** — "Activity가 끝났는지 신경 안 쓰고 Workflow 흐름을 빨리 다음으로 넘기고 싶을 때". 레이싱 패턴에서 **지는 Activity**를 버릴 때 자연스러움.

**`ActivityCancellationType.WAIT_CANCELLATION_COMPLETED`**
- Workflow는 **Activity가 cleanup을 끝내고 `CanceledFailure`를 다시 throw할 때까지** 블록.
- Activity가 heartbeat → `ActivityCompletionException` catch → cleanup 수행 → `throw e` → 서버가 이 완료를 Workflow Task에 전달 → Workflow가 `CanceledFailure` 받음.
- **heartbeat가 필수** — Activity가 heartbeat를 안 하면 cancel 요청을 못 받아 Workflow가 영원히 대기 (StartToCloseTimeout까지).
- **쓸 때** — "cleanup이 Workflow 다음 로직의 전제조건" (예: 리소스 반환 확인, DB 롤백 완료 확인). `core/.../hello/HelloCancellationScope.java` + `core/.../hello/HelloWorkflowTimer.java`에서 사용.

**`ActivityCancellationType.ABANDON`**
- Workflow는 **즉시** `CanceledFailure`를 받음 (TRY_CANCEL과 비슷).
- 차이점: **서버에 cancel 요청을 아예 보내지 않음**. Activity는 자기가 cancel 당한 걸 **모른 채** 끝까지 실행.
- Activity 완료 결과(성공/실패)는 서버가 받아 **드랍**(히스토리에 적재는 되지만 Workflow에 전달 안 됨).
- **쓸 때** — "이미 시작된 Activity를 중간에 끊어선 안 되지만, Workflow는 결과를 기다릴 필요 없음" — 예: 외부 시스템에 요청을 보냈는데 중단하면 상태가 꼬이는 경우. 멱등성 확보가 어려운 작업.

#### 세 가지 선택 비교표

| 항목 | **TRY_CANCEL** (기본) | **WAIT_CANCELLATION_COMPLETED** | **ABANDON** |
| --- | --- | --- | --- |
| Workflow가 `CanceledFailure` 받는 시점 | **즉시** | Activity cleanup 완료 후 | **즉시** |
| 서버가 Activity에 cancel 요청? | **O** | O | **X** |
| Activity cleanup 수행? | O (하지만 Workflow가 기다리지 않음) | O (Workflow가 기다림) | O (Activity 자체는 평소대로 끝까지 실행) |
| heartbeat 없으면 어떻게 되나? | Activity는 중단되지 않고 계속 실행 (StartToCloseTimeout까지) | **Workflow가 영원히 대기** | 상관없음 (요청 자체를 안 보냄) |
| Activity의 최종 결과가 Workflow에 전달? | **X** (버려짐) | O (`CanceledFailure` 또는 cleanup 중 던진 예외) | **X** (버려짐) |
| Workflow 전체 종료 속도 | 빠름 | 느림 (cleanup 시간) | 빠름 |
| 외부 리소스 cleanup 필요성 | Activity가 자체 처리 (Workflow는 모름) | **Workflow가 확인** 가능 | Activity가 자체 처리 (하지만 cancel 자체를 모름) |

#### Activity 쪽 코드 — heartbeat와 cleanup (`core/.../hello/HelloCancellationScope.java`)

```java
public String composeGreeting(String greeting, String name) {
    ActivityExecutionContext ctx = Activity.getExecutionContext();
    for (int i = 0; i < seconds; i++) {
        sleep(1);
        try {
            ctx.heartbeat(i);                              // 이게 없으면 cancel 알림 못 받음
        } catch (ActivityCompletionException e) {
            // cancel 또는 timeout 발생 — 여러 원인:
            //  1) Workflow가 scope.cancel 호출
            //  2) Activity가 서버 입장에서 timeout
            //  3) Worker shutdown
            performCleanup();                              // 리소스 반환, 커넥션 닫기 등
            throw e;                                       // ⚠️ 반드시 re-throw
        }
    }
    return greeting + " " + name + "!";
}
```

**중요 — `ActivityCompletionException`을 삼키면 안 된다.** catch만 하고 reth-row 안 하면 Activity가 "정상 완료"로 처리되어 Workflow가 성공 결과를 받는다. cancel의 의미가 사라짐.

#### `HeartbeatTimeout` 설정이 왜 중요한가

```java
ActivityOptions.newBuilder()
    .setHeartbeatTimeout(Duration.ofSeconds(5))     // ← 명시 안 하면 기본 ~30초로 throttle됨
    .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
    ...
```
`setHeartbeatTimeout` 없이 heartbeat만 호출하면 SDK가 **~30초 간격**으로 쓰로틀한다 (서버 부하 방지). cancel을 빨리 전달하려면 timeout을 짧게 설정 — Activity 코드가 heartbeat를 호출하면 그 즉시 서버와 통신해 cancel 신호를 가져옴.

#### Workflow 쪽 catch 패턴

```java
try {
    activities.doIt(x);
} catch (ActivityFailure e) {
    if (e.getCause() instanceof CanceledFailure) {
        // cancel로 끝났음. 보상/cleanup 로직
    } else {
        // 다른 원인 (ApplicationFailure, TimeoutFailure 등)
        throw e;
    }
}
```
`ActivityFailure`가 wrapper, 실제 원인은 `getCause()`로 확인.

#### 선택 가이드 — 상황별

| 상황 | 선택 |
| --- | --- |
| "레이싱해서 1등만 쓰고 나머지 버림" | **TRY_CANCEL** (기본) — 빠르게 다음 로직 |
| "리소스 cleanup까지 확인하고 다음으로" (DB 롤백 등) | **WAIT_CANCELLATION_COMPLETED** |
| "cleanup 결과에 따라 Workflow 분기해야 함" | **WAIT_CANCELLATION_COMPLETED** |
| "이미 외부 시스템에 돈이 송금됐는데 중간 중단은 재앙" | **ABANDON** — Activity는 끝까지, Workflow만 포기 |
| "cancel을 보내도 Activity가 heartbeat 안 하는 상황이 흔함" | **TRY_CANCEL** 또는 **ABANDON** — WAIT는 영원히 대기 가능 |
| "Workflow가 cancel된 뒤에도 cleanup Activity는 반드시 실행" | **ABANDON** 또는 cleanup을 **detached scope에서** 별도로 호출 (§14) |

#### `ChildWorkflowCancellationType`과의 차이

Child Workflow에는 **`WAIT_CANCELLATION_REQUESTED`** 라는 추가 값이 있어 **4개 값 체계**이고, 기본값도 `WAIT_CANCELLATION_COMPLETED`로 Activity와 반대다. 상세는 §11의 "`ChildWorkflowCancellationType` 심화" 참고.

#### `NexusOperationCancellationType`과의 차이

Nexus는 **`WAIT_COMPLETED`가 기본값**이다 (Activity와 반대). 네트워크 왕복과 외부 서비스 보장이 중요해 보수적인 기본값. 상세는 §13.

---

### Local Activity의 경우
`Workflow.newLocalActivityStub(...)`는 `LocalActivityOptions`를 받는다 — API는 거의 동일하지만 `ScheduleToStartTimeout`이 없고 (큐를 거치지 않음), `setLocalRetryThreshold(Duration)`가 추가된다. **Activity 실행 자체는 서버와 통신하지 않지만**, 결과는 Workflow Task가 완료될 때 marker로 함께 올라간다. 짧은 작업에 적합. 상세는 §12 참고.

---

## 8. `RetryOptions` — 지수 백오프 재시도 정책

### 역할
`ActivityOptions.setRetryOptions(...)` 또는 `WorkflowOptions.setRetryOptions(...)`에 넘기는 **지수 백오프 기반 재시도 설정**. 설정하지 않으면 Temporal 서버가 기본값(무제한 재시도, 1초 초기 간격, 2.0 배수, 100초 상한)을 적용한다. 명시적으로 끄거나 조이려면 반드시 전달한다.

### 백오프 공식
```
attempt N의 대기 시간 = min(InitialInterval × BackoffCoefficient^(N-1), MaximumInterval)
멈추는 조건            = MaximumAttempts 도달 OR 예외가 DoNotRetry 목록에 포함 OR ScheduleToCloseTimeout 초과
```

### 생성
```java
RetryOptions retry =
    RetryOptions.newBuilder()
        .setInitialInterval(Duration.ofSeconds(1))
        .setBackoffCoefficient(2.0)
        .setMaximumInterval(Duration.ofMinutes(1))
        .setMaximumAttempts(5)                          // 0 = 무한
        .setDoNotRetry(IllegalArgumentException.class.getName())
        .build();
```

### 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setInitialInterval(Duration)` | 첫 재시도 전 대기 시간 (기본 1s) |
| `setBackoffCoefficient(double)` | 재시도마다 간격을 곱할 배수 (기본 2.0). `1.0`이면 고정 간격 |
| `setMaximumInterval(Duration)` | 백오프 상한. 기본값은 `InitialInterval × 100` |
| `setMaximumAttempts(int)` | 총 시도 횟수. **`0` = 무한** (기본), **`1` = 재시도 없음**, N이면 최초 1회 + 재시도 N-1회 |
| `setDoNotRetry(String...)` | 재시도를 **건너뛸** 예외의 FQCN 목록. 비즈니스 오류(검증 실패 등) 격리에 사용 (`core/.../peractivityoptions/PerActivityOptionsTest.java`) |

### 샘플에서 자주 쓰이는 세 패턴

```java
// 1) "실패하면 즉시 포기" — 보상(compensation) Activity, 멱등성 없는 작업
RetryOptions.newBuilder().setMaximumAttempts(1).build();
// → core/.../bookingsyncsaga/TripBookingWorkflowImpl.java (compensationOptions)

// 2) "짧게 몇 번만" — LLM 호출, 외부 API 등 비용 있는 재시도
RetryOptions.newBuilder().setMaximumAttempts(3).build();
// → springai/rag/.../RagWorkflowImpl.java, springai/basic/.../ChatWorkflowImpl.java

// 3) "특정 예외는 즉시 fail, 나머지는 기본 백오프"
RetryOptions.newBuilder()
    .setDoNotRetry(NullPointerException.class.getName())
    .build();
```

> **주의** — `setDoNotRetry(...)`는 **클래스 FQCN 문자열**을 받는다. `IllegalArgumentException.class.getName()`처럼 전달. 클래스 리터럴을 그대로 넘기면 컴파일 에러.
>
> **ScheduleToCloseTimeout과 상호작용** — 재시도 중이라도 `ScheduleToCloseTimeout`이 지나면 중단된다. "최대 N번 시도 OR 최대 M분 소요" 중 먼저 걸리는 쪽이 이긴다.

### `RetryOptions` 심화

#### 어디에 넘길 수 있는가 — 5곳

RetryOptions는 **여러 다른 context**에서 동일한 Builder를 재사용한다. 어디에 넘기냐에 따라 "무엇을 재시도하는지"가 달라짐:

| 설정 지점 | 재시도 대상 | 기본값 | 설정 지점 |
| --- | --- | --- | --- |
| `ActivityOptions.setRetryOptions(...)` | **Activity 실행 1건** | 무제한 재시도 | §7 |
| `LocalActivityOptions.setRetryOptions(...)` | **Local Activity 실행 1건** | 무제한 재시도 (워커 안에서) | §12 |
| `WorkflowOptions.setRetryOptions(...)` | **Workflow Execution 전체** | 재시도 안 함 (null) | §6 |
| `ChildWorkflowOptions.setRetryOptions(...)` | **자식 Workflow Execution** | 재시도 안 함 (null) | §11 |
| `Async.retry(RetryOptions, Optional<Duration>, Supplier)` | **임의 람다** (여러 Activity 묶음 등) | 명시 필수 | §15 |

> **Workflow/ChildWorkflow 기본은 "재시도 안 함"** — 이름이 똑같은 RetryOptions지만 Activity와 기본 동작이 완전히 다르다. Workflow를 재시도하려면 **명시적으로** `RetryOptions`를 설정해야 Workflow 레벨 재시도가 걸림. (대부분의 경우 Activity 재시도만으로 충분해서 Workflow 재시도는 거의 안 쓴다.)

#### 기본값 명세 (Activity 기준)

| 옵션 | 기본값 | 서버가 이렇게 둔 이유 |
| --- | --- | --- |
| `InitialInterval` | **1초** | 즉시 재시도는 문제 반복 → 1초 간격이 "숨 돌리기" 적정 |
| `BackoffCoefficient` | **2.0** | 지수 백오프의 표준 (매번 2배) |
| `MaximumInterval` | **InitialInterval × 100** = 100초 | 극단적 폭주 방지 상한 |
| `MaximumAttempts` | **0 (무한)** | Temporal 철학: "결국 성공할 때까지 돌림" |
| `DoNotRetry` | 비어 있음 | 모든 예외 재시도 |

→ **아무것도 설정 안 하면** `1s → 2s → 4s → 8s → 16s → 32s → 64s → 100s → 100s → ...` 무한.

#### 백오프 시각화

`InitialInterval=1s`, `BackoffCoefficient=2.0`, `MaximumInterval=60s` 기준 **첫 10회 간격**:

| 시도 N | 대기 (수식) | 대기 (실제) | 누적 |
| --- | --- | --- | --- |
| 1 → 2 | `1 × 2^0` | 1s | 1s |
| 2 → 3 | `1 × 2^1` | 2s | 3s |
| 3 → 4 | `1 × 2^2` | 4s | 7s |
| 4 → 5 | `1 × 2^3` | 8s | 15s |
| 5 → 6 | `1 × 2^4` | 16s | 31s |
| 6 → 7 | `1 × 2^5` | 32s | 1분 3s |
| 7 → 8 | `min(1 × 2^6, 60)` | **60s** (상한) | 2분 3s |
| 8 → 9 | 60s | 60s | 3분 3s |
| 9 → 10 | 60s | 60s | 4분 3s |
| 10 → 11 | 60s | 60s | 5분 3s |

→ 상한까지 **빠르게 올라가고**, 그 뒤는 **상한에 고정**. "실패하면 몇 시간 뒤에나 재시도"를 원하면 `MaximumInterval`을 크게.

> **참고 — jitter는 서버가 자동으로 추가.** 간격에 ±10% 정도의 무작위 지연이 들어가 "동시에 수백 개가 재시도"를 분산시킴. SDK/옵션으로 제어 불가.

#### `MaximumAttempts` 의미 정확히

```
MaximumAttempts = 0  → 무한 재시도 (기본)
MaximumAttempts = 1  → 재시도 없음 (최초 1회만)
MaximumAttempts = 2  → 최초 1회 + 재시도 1회 = 총 2회
MaximumAttempts = 5  → 최초 1회 + 재시도 4회 = 총 5회
```
"재시도 횟수"가 아니라 **"총 시도 횟수"**. 이 혼동이 빈번.

#### `DoNotRetry` — 재시도 건너뛰기 메커니즘

**FQCN 문자열 매칭**:
- `setDoNotRetry("java.lang.IllegalArgumentException")` — 정확 일치
- 상속은 자동 매칭 안 됨 — subclass도 FQCN을 명시해야 함

**Temporal이 던지는 예외와의 관계**:
- Activity 코드에서 `throw new IllegalArgumentException(...)`
- Temporal이 자동으로 **`ApplicationFailure`로 래핑**해 서버에 보냄
- 서버가 Failure의 **type 필드**를 보고 DoNotRetry에 매칭
- `ApplicationFailure.getType()` 값과 매칭되므로, 커스텀 `ApplicationFailure.newFailure("MyCustomType", ...)`를 쓰면 `setDoNotRetry("MyCustomType")`로 매칭

#### 코드 레벨 non-retryable — `ApplicationFailure.newNonRetryableFailure(...)`

`setDoNotRetry`는 **옵션 레벨**에서 "어떤 타입은 재시도 안 함"을 선언. **코드 레벨**에서 "이 호출은 재시도 안 함"을 던지려면:

```java
// Activity 코드 안에서
if (!valid(input)) {
    throw ApplicationFailure.newNonRetryableFailure(
        "Invalid input: " + input,     // 메시지
        "ValidationError");             // type
}
```
→ `setDoNotRetry`에 등록 안 돼도 **즉시 재시도 중단**. 비즈니스 오류를 호출자에 전파하는 표준 패턴.

**비교:**

| 수단 | 선언 지점 | 용도 |
| --- | --- | --- |
| `setDoNotRetry(FQCN...)` | **ActivityOptions** (호출자가 선언) | "이 타입은 Activity 전반에서 재시도 안 함" |
| `ApplicationFailure.newNonRetryableFailure` | **Activity 코드** (피호출자가 선언) | "이 상황은 재시도해도 소용없음" (비즈니스 오류) |

→ 보통 **ApplicationFailure 쪽을 더 자주** 쓴다. Activity 코드가 "이건 재시도 소용 없는 오류"를 가장 잘 안다.

#### 재시도 중단 조건 통합 다이어그램

```
          Attempt N 실패
                │
                ▼
  ┌─────────────────────────────┐
  │ 예외가 DoNotRetry에 포함?    │ Yes → 중단 (실패 전파)
  └─────────────────────────────┘
                │ No
                ▼
  ┌─────────────────────────────┐
  │ Non-retryable ApplicationFailure? │ Yes → 중단
  └─────────────────────────────┘
                │ No
                ▼
  ┌─────────────────────────────┐
  │ MaximumAttempts 도달?        │ Yes → 중단 (실패 전파)
  └─────────────────────────────┘
                │ No
                ▼
  ┌─────────────────────────────┐
  │ ScheduleToCloseTimeout 초과? │ Yes → 중단 (TimeoutFailure)
  └─────────────────────────────┘
                │ No
                ▼
       백오프 대기 (next interval)
                │
                ▼
          Attempt N+1
```

#### 타임아웃과의 상호작용 — 어느 쪽이 이기나

Activity 실패 → 재시도 중단 조건을 **4개 축**이 동시에 감시. **먼저 걸리는 쪽**이 이긴다:

| 축 | 체크 대상 |
| --- | --- |
| `MaximumAttempts` | 총 시도 횟수 |
| `ScheduleToCloseTimeout` | 재시도 포함 **총 시간** |
| `StartToCloseTimeout` | **1회 시도** 시간 (초과 시 그 시도는 실패, 재시도는 계속) |
| `DoNotRetry` + `nonRetryable` | 예외 타입/플래그 |

**실전 조합 예:**

```java
ActivityOptions.newBuilder()
    .setStartToCloseTimeout(Duration.ofMinutes(2))              // 1회 시도 2분
    .setScheduleToCloseTimeout(Duration.ofHours(1))             // 전체 1시간 안에 결판
    .setRetryOptions(RetryOptions.newBuilder()
        .setMaximumAttempts(10)                                  // 최대 10회
        .setInitialInterval(Duration.ofSeconds(1))
        .setBackoffCoefficient(2.0)
        .setMaximumInterval(Duration.ofMinutes(5))
        .setDoNotRetry("ValidationError")                        // 검증 오류 즉시 중단
        .build())
    .build();
```
→ "**10회 OR 1시간** 중 먼저 걸리는 쪽, **ValidationError**면 즉시, 각 시도는 **2분** 안에 끝나야 함."

#### 서버가 던지는 Failure 타입과의 상호작용

Activity 재시도가 끝나면 Workflow 코드가 받는 예외는:

| 상황 | 예외 타입 (wrapper) | 원인(`getCause()`) |
| --- | --- | --- |
| 재시도 소진 (MaxAttempts) | `ActivityFailure` | 마지막 시도의 `ApplicationFailure` |
| `nonRetryable` 플래그 | `ActivityFailure` | `ApplicationFailure` (nonRetryable) |
| `DoNotRetry` 매칭 | `ActivityFailure` | `ApplicationFailure` |
| `ScheduleToCloseTimeout` 초과 | `ActivityFailure` | **`TimeoutFailure`** |
| `StartToCloseTimeout` 초과 (재시도 가능) | 재시도 계속 — Workflow엔 안 올라옴 | — |
| `Workflow.sleep` 중 cancel | `ActivityFailure` | **`CanceledFailure`** |

→ Workflow에서 `catch (ActivityFailure e) { e.getCause() ... }` 패턴이 그래서 자주 나옴.

#### 안티패턴

1. **`InitialInterval=0` 또는 아주 작게** — 즉시 재시도가 서버·외부 시스템에 폭풍처럼 요청 쇄도. 1초 미만 비추천.
2. **`BackoffCoefficient=1.0` + 짧은 interval** — 간격이 안 늘어나 "실패하면 1초마다 영원히 재시도"가 됨. 문제가 지속되는 상황에서 재앙.
3. **`MaximumAttempts=1` 설정으로 "재시도 끔"** — 간혹 혼동. "1 = 재시도 없음, 0 = 무한". **0이 끄는 게 아니다**.
4. **`DoNotRetry`에 `RuntimeException` 등록** — 모든 unchecked 예외가 매칭돼 재시도 전부 중단. 과도하게 광범위.
5. **코드가 `throw new RuntimeException("validation failed")`** — 재시도됨. 대신 `ApplicationFailure.newNonRetryableFailure`로.
6. **Workflow에 `setRetryOptions` 설정 안 하고 "자동 재시도"를 기대** — Workflow 레벨 재시도는 **기본 끄져 있음**. Activity와 다름.
7. **`ScheduleToCloseTimeout` 없이 무한 재시도** — Activity가 영원히 재시도되며 Workflow가 stuck. `MaximumAttempts`나 ScheduleToClose 중 하나는 걸어두기.
8. **네트워크 오류와 비즈니스 오류를 같은 재시도 정책으로** — 네트워크는 지수 백오프로 재시도, 비즈니스 검증 실패는 `nonRetryable`. 섞어 쓰면 UX 혼란.

#### 선택 가이드 — 상황별

| 상황 | RetryOptions |
| --- | --- |
| 멱등 외부 API 호출 | 기본값 (무한 백오프) + `ScheduleToCloseTimeout`으로 상한 |
| LLM/과금 API | `MaximumAttempts(3)` + 짧은 interval |
| 보상(compensation) Activity | **`MaximumAttempts(1)`** (재시도 없음) + 비즈니스 오류 격리 |
| 검증 Activity | 기본값 + 코드에서 `ApplicationFailure.newNonRetryableFailure` |
| 폴링 Activity (서버 응답 대기) | `BackoffCoefficient=1.0` + 짧은 고정 interval (`setMaximumInterval == setInitialInterval`) |
| 서비스 재시작 중 안정화 대기 | 긴 `MaximumInterval` + 큰 `MaximumAttempts` |
| Workflow 전체 재시도 | 거의 안 함. 하면 작은 `MaximumAttempts(2)` 정도로 "한 번만 재시도" |

#### Visibility / 디버깅

- Activity 재시도 횟수는 **UI Activity 라인**에 "Attempt N / M" 표시.
- `Activity.getExecutionContext().getInfo().getAttempt()` — 코드에서 현재 시도 횟수 접근 (**멱등성 로직에 유용**).
- `isLocalActivity()` — Local vs 일반 Activity 분기.
- 시도마다 다른 로그가 필요하면 `Workflow.getLogger(cls)` 사용 — 리플레이 로그 억제됨.

#### 샘플 레퍼런스 (RetryOptions만)

- `MaximumAttempts(1)` — 보상 Activity: `core/src/main/java/io/temporal/samples/bookingsaga/TripBookingWorkflowImpl.java`, `.../bookingsyncsaga/TripBookingWorkflowImpl.java`
- `MaximumAttempts(3)` — LLM / 외부 API: `springai/rag/src/main/java/io/temporal/samples/springai/rag/RagWorkflowImpl.java`, `springai/basic/src/main/java/io/temporal/samples/springai/chat/ChatWorkflowImpl.java`
- `DoNotRetry(FQCN)` — 비즈니스 예외 격리: `core/src/test/java/io/temporal/samples/peractivityoptions/PerActivityOptionsTest.java`

---
## 9. `WorkflowImplementationOptions` — 워커에 Workflow 등록할 때의 옵션

### 역할
`worker.registerWorkflowImplementationTypes(...)` 호출 시 **구현체 클래스에 함께 결속**되는 옵션. 특정 Workflow 구현의 **기본 Activity 옵션**, **Workflow를 실패로 처리할 예외 타입**, **Nexus 서비스 매핑** 등 "워커에서 이 Workflow를 실행할 때의 정책"을 지정한다. 설정 없이 등록해도 되지만, 아래 두 유즈케이스가 특히 자주 쓰인다.

### 생성과 등록
```java
WorkflowImplementationOptions options =
    WorkflowImplementationOptions.newBuilder()
        .setFailWorkflowExceptionTypes(InvalidCustomerException.class)
        .setActivityOptions(Map.of(
            "ActivityTypeA", ActivityOptions.newBuilder()
                .setScheduleToCloseTimeout(Duration.ofSeconds(5)).build()))
        .build();

worker.registerWorkflowImplementationTypes(options, MyWorkflowImpl.class);
// options 없이 등록할 수도 있음:
worker.registerWorkflowImplementationTypes(OtherWorkflowImpl.class);
```

### 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setFailWorkflowExceptionTypes(Class<? extends Throwable>...)` | 기본 동작(워크플로 Task 재시도)이 아니라 **Workflow Execution을 즉시 실패**시킬 예외 타입. 비즈니스 오류를 영구 실패로 처리할 때 사용 (`core/.../encodefailures/Starter.java`, `core/.../hello/HelloSignalWithStartAndWorkflowInit.java`) |
| `setActivityOptions(Map<String, ActivityOptions>)` | **Activity 타입명 → ActivityOptions** 매핑. 코드 안에서 `Workflow.newActivityStub`에 옵션을 안 넘겨도 여기 매핑이 적용됨 (`core/.../peractivityoptions/Starter.java`) |
| `setDefaultActivityOptions(ActivityOptions)` | 위 매핑에 없는 **모든 Activity 타입의 기본값** |
| `setLocalActivityOptions(Map<String, LocalActivityOptions>)` | 위와 동일하지만 Local Activity용 |
| `setDefaultLocalActivityOptions(LocalActivityOptions)` | 모든 Local Activity 기본값 |
| `setNexusServiceOptions(Map<String, NexusServiceOptions>)` | **Nexus 서비스명 → NexusServiceOptions** 매핑. 테스트에서 Nexus endpoint 바꿔치기할 때 자주 사용 (`core/.../nexus/caller/CallerWorkflowTest.java`) |
| `setEnableUpsertVersionSearchAttributes(boolean)` | `Workflow.getVersion(...)` 호출 시 자동으로 `TemporalChangeVersion` 검색 attribute 추가 여부 |

### Workflow 코드의 `ActivityOptions`와의 우선순위

Activity 옵션은 세 계층에서 지정 가능하다. **더 좁은 범위가 이긴다**:

```
Workflow.newActivityStub(T.class, EXPLICIT_OPTIONS)   ← 최우선
          ▲
WorkflowImplementationOptions.setActivityOptions(Map)  ← 코드가 옵션 없이 stub을 만들 때
          ▲
WorkflowImplementationOptions.setDefaultActivityOptions(...) ← 그마저도 없을 때
```

→ 즉, Workflow 코드가 `Workflow.newActivityStub(T.class)`처럼 옵션을 **생략**해도, 워커 등록 시 매핑이 있으면 그 값이 적용된다. 샘플 운영 환경에서 Activity 설정을 외부(배포 설정)로 빼는 패턴.

### `setFailWorkflowExceptionTypes` 참고

기본 Temporal 동작은 Workflow 코드에서 throw된 예외를 **Workflow Task 실패**로 보고 **재시도**한다 (일시적 버그로 가정). 비즈니스 규칙 위반 같은 영구적 오류는 재시도해도 소용 없으므로, 해당 예외 타입을 여기 등록해 **Workflow Execution 자체를 실패**로 종료시킨다.

```java
// InvalidCustomerException이 던져지면 → Workflow가 FAILED 상태로 종료 (재시도 없음)
WorkflowImplementationOptions.newBuilder()
    .setFailWorkflowExceptionTypes(InvalidCustomerException.class)
    .build();
```

---
## 12. `LocalActivityOptions` — 로컬 실행 Activity 전용 옵션

### 역할
`Workflow.newLocalActivityStub(...)`에 넘기는 옵션. **일반 Activity와 달리 서버 큐(TaskQueue)를 거치지 않고 워크플로를 실행 중인 바로 그 워커의 JVM에서 즉시 실행**된다. 지연과 서버 호출 횟수가 적은 대신, 원격 큐·분산 재시도·heartbeat 같은 서버 기능을 포기한다.

### 서버와는 정말 통신 안 하나?

"Local = 서버와 통신 X" 는 **반쪽만 맞다.** 정확히는 세 단계로 나뉜다.

1. **Activity 실행 중** — 서버와 통신하지 않음. 일반 Activity의 `ScheduleActivityTask` / `ActivityTaskStarted` / `ActivityTaskCompleted` 3개 이벤트와 TaskQueue 폴링 왕복이 전부 생략된다. 그래서 heartbeat API도 없다.
2. **감싸고 있는 Workflow Task가 완료될 때** — Local Activity의 입력·결과·예외가 `MarkerRecorded` 이벤트로 **히스토리에 함께 적재**된다. 이 적재가 리플레이 결정성(replay determinism)을 보장한다 — 리플레이 때는 marker를 읽어 결과를 그대로 돌려주고 실제 재실행하지 않는다. 즉 "서버 왕복이 Local Activity 호출 수만큼 늘지는 않는다"는 뜻이지, 결과가 서버로 안 간다는 뜻은 아니다.
3. **재시도가 `LocalRetryThreshold`를 넘길 때** — SDK가 현재 Workflow Task를 강제로 완료시키고 **Timer를 서버에 걸어 재시도 상태를 히스토리에 적재**한다. Timer가 fire되면 새 Workflow Task에서 재시도가 이어진다. 이 경우 Workflow Task 완료 ↔ Timer fire 왕복이 반복되므로 서버 통신이 여러 번 발생한다. 긴 재시도가 Local Activity와 어울리지 않는 이유.

### 생성 (`core/.../hello/HelloLocalActivity.java`, `springboot/.../customize/CustomizeWorkflowImpl.java`)
```java
private final GreetingActivities activities =
    Workflow.newLocalActivityStub(
        GreetingActivities.class,
        LocalActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(2))
            .build());
```

### 언제 쓰고 언제 쓰지 말아야 하는지

**쓰면 좋은 경우** — 짧고(수백 ms 이내), 서버 왕복이 아까운 작업. 로그 적재, 간단한 변환, in-process 유효성 검사, 이미 완료된 결과 조회. `core/.../bookingsyncsaga/TripBookingWorkflowImpl.java`는 happy path 전체를 **단일 Workflow Task 안에서** 끝내려고 Local Activity로 묶는다 (주석: "Don't use local activities if you expect long retries").

**피해야 할 경우** —
- **장시간 재시도** 필요 (워커가 죽으면 재시도 상태가 함께 사라짐)
- **큰 입력/출력**: 전체가 Workflow 히스토리에 marker로 적재됨
- **멱등성 없는 네트워크 호출**: 워커 재시작 중 재실행 가능성
- **heartbeat** 필요 (Local Activity는 heartbeat API 없음)

### `LocalActivityOptions` 주요 옵션

| 옵션 | 용도 |
| --- | --- |
| `setStartToCloseTimeout(Duration)` | **1회 시도** 최대 실행 시간. 이것 또는 `ScheduleToCloseTimeout` 중 하나는 필수 |
| `setScheduleToCloseTimeout(Duration)` | 재시도 포함 전체 상한. 로컬이라도 재시도 체인이 이 시간을 넘기면 중단 |
| `setRetryOptions(RetryOptions)` | 재시도 정책. 서버가 아닌 **워커 내부**에서 백오프하며 재시도. 긴 재시도는 Local Activity와 맞지 않음 |
| `setLocalRetryThreshold(Duration)` | 이 시간 안에 재시도가 끝나지 않으면 **Workflow Task에 Timer를 걸고 중단한 뒤 재시도 상태를 히스토리에 적재**. 긴 재시도도 어느 정도 안전해지지만 오버헤드 발생 |
| `setDoNotIncludeArgumentsIntoMarker(boolean)` | `true`로 두면 입력 인자를 히스토리 marker에 넣지 않음. **입력이 크거나 민감할 때 필수** (단, 디버깅은 어려워짐) |

### 일반 `ActivityOptions`와의 차이

| 항목 | `ActivityOptions` (일반) | `LocalActivityOptions` |
| --- | --- | --- |
| 실행 위치 | TaskQueue에 큐잉 → 아무 워커가 폴링해서 실행 | 현재 Workflow를 돌리는 바로 그 워커의 JVM 안에서 즉시 실행 |
| 실행 중 서버 통신 | **매 호출마다** `ScheduleActivityTask` + `ActivityTaskStarted` + `ActivityTaskCompleted` 3이벤트 + 폴링 왕복 | **없음** — Activity 호출 자체는 서버를 거치지 않음 |
| 결과 적재 | 각 Activity마다 `ActivityTaskCompleted` 이벤트로 히스토리에 기록 | 감싸는 **Workflow Task 완료 시** `MarkerRecorded` 이벤트로 함께 적재 (호출 수만큼 왕복이 늘지는 않음) |
| 긴 재시도 | 서버가 백오프·재스케줄 관리, 워커가 죽어도 안전 | `LocalRetryThreshold` 초과 시 Workflow Task 완료 + Timer로 전환 → 서버 왕복 추가 발생 |
| `ScheduleToStartTimeout` | O | **X** (큐가 없음) |
| `Heartbeat` | O | **X** (실행 중 서버와 통신 안 함) |
| `TaskQueue` 지정 | O | X (현재 Workflow와 동일) |
| `CancellationType` | O | 지원되지만 사실상 즉시 반환 |
| 워커 죽음 시 재시도 | 서버가 다른 워커에 재할당 | **없음** — 현재 Workflow Task가 재시도되며 처음부터 다시 실행 |

### 샘플 레퍼런스
- 기본 사용: `core/src/main/java/io/temporal/samples/hello/HelloLocalActivity.java`
- Happy path를 Local로, 보상은 일반 Activity로 분리: `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java`
- Spring Boot에서 Local과 일반 Activity 혼용: `springboot/src/main/java/io/temporal/samples/springboot/customize/CustomizeWorkflowImpl.java`
- Update handler + Local Activity: `springboot/src/main/java/io/temporal/samples/springboot/update/PurchaseWorkflowImpl.java`
