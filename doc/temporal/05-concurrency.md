# 동시성 — CancellationScope / Async / Promise

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 14. `CancellationScope` — async 작업 묶음을 함께 취소

### 역할
Workflow 코드 안에서 **Activity / Timer / ChildWorkflow / Nexus operation을 그룹으로 묶어 한꺼번에 취소**할 수 있게 해주는 헬퍼. Options 클래스가 아니라 `io.temporal.workflow.CancellationScope`라는 런타임 객체. 지금까지 다룬 `ActivityOptions.setCancellationType` / `ChildWorkflowOptions.setCancellationType` / `NexusOperationOptions.setCancellationType`이 "취소 신호를 받으면 어떻게 반응할지"를 정했다면, Scope는 "그 신호를 **누구에게** 쏠지"를 정한다.

### 두 종류

```java
Workflow.newCancellationScope(() -> { ... })          // 일반 scope — 부모 scope 취소가 전파됨
Workflow.newDetachedCancellationScope(() -> { ... })  // detached scope — 부모 취소와 무관하게 끝까지 실행
```

워크플로 전체도 하나의 **암묵적 root scope**다. 그래서 외부에서 `WorkflowStub.cancel()`을 호출하면 내부 모든 scope가 함께 취소된다 — detached scope는 제외.

### 사용 패턴 1 — 레이싱 후 나머지 취소

`core/.../hello/HelloCancellationScope.java`, `core/.../nexuscancellation/caller/HelloCallerWorkflowImpl.java`

```java
List<Promise<String>> results = new ArrayList<>();
CancellationScope scope = Workflow.newCancellationScope(() -> {
    for (String g : greetings) {
        results.add(Async.function(activities::composeGreeting, g, name));
    }
});
scope.run();                                   // scope 안의 Async들을 동시에 시작
String winner = Promise.anyOf(results).get();  // 하나만 끝날 때까지 대기
scope.cancel();                                // 나머지 전부 취소
```

> `Async.function(...)`은 **scope 안에서 호출되어야 그 scope에 속한다.** scope 바깥에서 시작한 작업에는 `scope.cancel()`이 영향을 주지 않는다.

### 사용 패턴 2 — Timer로 Activity에 timeout 걸기

`core/.../hello/HelloCancellationScopeWithTimer.java`

```java
CancellationScope activityScope = Workflow.newCancellationScope(() -> {
    result = activities.updateInfo(input);     // 길 수 있는 Activity
});

Workflow.newTimer(Duration.ofSeconds(3))        // 3초 내 안 끝나면
    .thenApply(r -> { activityScope.cancel(); return null; });  // 취소

try { activityScope.run(); }
catch (ActivityFailure e) {
    if (e.getCause() instanceof CanceledFailure) result = "default (timeout)";
}
```
→ Activity의 `StartToCloseTimeout`을 바꾸지 않고도 "N초 넘기면 포기하고 기본값으로" 패턴을 구현. 일반 scope + Timer + `scope.cancel()` 조합.

### 사용 패턴 3 — 보상(cleanup)이 cancel에 휩쓸리지 않게

`core/.../bookingsyncsaga/TripBookingWorkflowImpl.java`, `core/.../bookingsaga/TripBookingWorkflowImpl.java`, `core/.../hello/HelloDetachedCancellationScope.java`

```java
try {
    activities.doSomething();
} catch (ActivityFailure e) {
    // ❌ saga.compensate() 그냥 호출하면, 외부에서 워크플로를 cancel한 상황에서
    //    보상 Activity도 함께 취소되어 유실됨.
    // ✅ detached scope로 감싸면 cancel 영향 없이 끝까지 실행.
    Workflow.newDetachedCancellationScope(() -> saga.compensate()).run();
    throw e;
}
```
→ **Saga 보상·DB 롤백·notification 발송처럼 "반드시 끝나야 하는 cleanup"은 detached scope가 사실상 필수.** §10의 Saga 샘플 3개 모두 이 패턴.

### 사용 패턴 4 — Signal/Update로 특정 작업만 취소

`core/.../hello/HelloUpdateAndCancellationTest.java`

```java
private CancellationScope scope;         // 필드로 노출

public String execute() {
    scope = Workflow.newCancellationScope(() -> activities.runActivity());
    scope.run();
    ...
}

@SignalMethod
public void cancelIt() { scope.cancel(); }   // signal에서 선택적 취소
```
→ 전체 워크플로를 취소하는 게 아니라 **특정 작업만** signal/update로 끊는 패턴.

### cancel이 실제로 동작하는 방식

`scope.cancel()`은 **협력적(cooperative)**이다 — 신호만 쏘고, 각 작업이 어떻게 반응할지는 **그 작업의 Options**가 결정한다.

| 작업 | 반응을 결정하는 옵션 | 특이사항 |
| --- | --- | --- |
| Activity | `ActivityOptions.setCancellationType(...)` | Activity 코드는 **heartbeat**를 호출해야 취소 알림을 받음 (`ActivityCompletionException` throw). `core/.../hello/HelloCancellationScope.java`의 Activity 코드 참고 |
| ChildWorkflow | `ChildWorkflowOptions.setCancellationType(...)` | 자식에게 cancel 요청 전파 |
| Nexus operation | `NexusOperationOptions.setCancellationType(...)` | handler에게 cancel 전파 |
| Timer (`Workflow.sleep`, `Workflow.newTimer`) | 옵션 없음 | 즉시 `CanceledFailure` throw |

### 핵심 API

| 호출 | 설명 |
| --- | --- |
| `Workflow.newCancellationScope(Runnable)` | 새 scope 생성 (부모 scope에 연결) |
| `Workflow.newDetachedCancellationScope(Runnable)` | 독립 scope 생성 (부모 cancel 영향 안 받음) |
| `scope.run()` | scope 안의 코드 실행. async 작업을 포함하면 **동기적으로 실행하되 블록하지 않음** (Promise는 백그라운드에서 진행) |
| `scope.cancel()` | scope 전체 취소. 안쪽의 모든 async 작업에 cancel 신호 전파 |
| `scope.cancel(String reason)` | 취소 사유 문자열 포함 (디버깅용) |
| `scope.isCancelRequested()` | 취소 요청 들어왔는지 확인 |
| `CancellationScope.current()` | 현재 scope 참조 (고급 패턴용) |

### 자주 하는 실수

1. **detached 없이 catch 블록에서 cleanup 호출** — 외부 워크플로 cancel 상황에서 cleanup Activity가 즉시 함께 취소된다. Saga 보상이 유실되는 전형적 원인. 반드시 `Workflow.newDetachedCancellationScope(() -> cleanup()).run()`.
2. **Async 호출을 scope 바깥에서 시작하고 scope.cancel 기대** — scope는 **문법적 범위(lexical scope)**가 아니라 **Async 시작 시점의 current scope**로 소속이 결정된다. scope.run() 람다 안에서 `Async.function(...)`을 호출해야 그 scope에 속한다.
3. **Activity에 heartbeat 없음** — cancel 신호가 전달되지 않아 Activity가 `StartToCloseTimeout`까지 돌아버린다. 긴 Activity는 반드시 `setHeartbeatTimeout(...)` + `context.heartbeat(...)` 호출.
4. **`WAIT_CANCELLATION_COMPLETED`로 걸어놓고 cleanup 대기 안 함** — 레이싱 패턴에서 `scope.cancel()` 호출 후 결과를 바로 반환해버리면 cleanup이 끝나기 전에 워크플로가 종료된다. `core/.../hello/HelloCancellationScope.java`처럼 `results` Promise들을 for-loop로 한 번씩 `.get()` 호출해 완료를 기다려야 함.

### 샘플 레퍼런스
- 레이싱(Activity): `core/src/main/java/io/temporal/samples/hello/HelloCancellationScope.java`
- 레이싱(Nexus operation): `core/src/main/java/io/temporal/samples/nexuscancellation/caller/HelloCallerWorkflowImpl.java`
- Timer로 Activity timeout 구현: `core/src/main/java/io/temporal/samples/hello/HelloCancellationScopeWithTimer.java`
- detached scope — 보상 보호: `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/bookingsaga/TripBookingWorkflowImpl.java`
- detached scope — cleanup Activity: `core/src/main/java/io/temporal/samples/hello/HelloDetachedCancellationScope.java`
- Signal로 scope.cancel: `core/src/test/java/io/temporal/samples/hello/HelloUpdateAndCancellationTest.java`
- Nexus handler 측 scope: `core/src/main/java/io/temporal/samples/nexuscancellation/handler/HelloHandlerWorkflowImpl.java`

---
## 15. `Async` — Workflow 안에서 비동기로 호출하기

### 역할
워크플로 코드 안에서 **Activity / ChildWorkflow / Nexus operation**을 호출할 때 기본은 **블로킹 동기 호출**(결과 반환 시까지 대기)이다. `Async`는 그 호출을 **즉시 반환하는 비동기 호출**로 바꿔 `Promise`를 돌려준다. 여러 호출을 fan-out하거나 Timer와 레이싱할 때 필수.

> ⚠️ **`CompletableFuture`나 `ExecutorService`를 쓰지 않는다** — Workflow 안에서는 `java.util.concurrent`를 직접 쓰면 결정성(determinism)이 깨진다. 비동기는 전부 `Async` + `Promise`로.

### 세 가지 메서드

| 메서드 | 반환 | 용도 |
| --- | --- | --- |
| `Async.function(func, args...)` | `Promise<R>` | **값을 반환**하는 Activity/ChildWorkflow/Nexus op 호출 (`core/.../hello/HelloChild.java`, `.../batch/slidingwindow/BatchWorkflowImpl.java`) |
| `Async.procedure(proc, args...)` | `Promise<Void>` | **void 반환** 호출 (`core/.../batch/iterator/IteratorBatchWorkflowImpl.java`, `.../batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`) |
| `Async.retry(retryOptions, expiration, func)` | `Promise<R>` | **임의의 람다를** 재시도 정책에 따라 비동기로 재실행 (일반 코드/local 함수 재시도용) |

### 사용 패턴 1 — Activity fan-out (`core/.../hello/HelloCancellationScope.java`)

```java
List<Promise<String>> results = new ArrayList<>();
for (String greeting : greetings) {
    results.add(Async.function(activities::composeGreeting, greeting, name));  // 즉시 반환
}
// 여기까지 반복문이 끝나도 아직 Activity는 전부 "시작만" 된 상태
```

### 사용 패턴 2 — ChildWorkflow 비동기 시작 (`core/.../asyncchild/ParentWorkflowImpl.java`)

```java
ChildWorkflow child = Workflow.newChildWorkflowStub(ChildWorkflow.class, opts);
Async.function(child::executeChild);                               // Promise는 버림 — 결과 대기 안 함
Promise<WorkflowExecution> started = Workflow.getWorkflowExecution(child); // 시작 확인
started.get();                                                      // 자식이 "시작됐음"만 확인
// 이 시점에서 부모가 return해도 자식은 계속 돌아감 (ParentClosePolicy.ABANDON과 함께)
```

### 사용 패턴 3 — void 호출을 Promise로 (`core/.../batch/iterator/IteratorBatchWorkflowImpl.java`)

```java
Promise<Void> result = Async.procedure(processor::processRecord, record);  // void 리턴을 Promise<Void>로
results.add(result);
// 나중에 Promise.allOf(results).get() 으로 전부 대기
```

### 사용 패턴 4 — `Async.retry`로 임의 람다 재시도

```java
Promise<Result> p = Async.retry(
    RetryOptions.newBuilder().setMaximumAttempts(5).build(),
    Optional.of(Duration.ofMinutes(10)),      // 전체 만료 시간
    () -> Async.function(activities::doStep)  // 재시도할 실제 호출
);
```
→ Activity 자체의 `RetryOptions`로 안 되는 복합 로직(여러 Activity를 묶어 재시도) 재시도에 사용.

### Scope와의 관계

`Async.function`은 **호출 시점의 current CancellationScope**에 속한다. §14의 레이싱 패턴이 이걸 활용 — scope 람다 안에서 `Async.function`을 호출해야 `scope.cancel()`로 함께 취소된다. **scope 바깥에서 시작한 Async는 그 scope에 안 속한다.**

---
## 16. `Promise` — Workflow 안의 비동기 결과 핸들

### 역할
`Async` 호출의 결과, Timer, Signal, ChildWorkflow 실행 등 **"아직 안 끝난 작업의 결과"를 나타내는 핸들**. `java.util.concurrent.CompletableFuture`의 Temporal 버전 — 다만 **워크플로 결정성이 보장되는 가상 스레드 위에서 동작**하므로 `CompletableFuture`를 Workflow 코드에 섞어 쓰면 안 된다.

### 어디서 생기나

| 소스 | 예 |
| --- | --- |
| `Async.function` / `Async.procedure` | `Promise<R> p = Async.function(activities::doIt, arg);` |
| `Workflow.newTimer(Duration)` | `Promise<Void> t = Workflow.newTimer(Duration.ofSeconds(3));` |
| `Workflow.getWorkflowExecution(childStub)` | `Promise<WorkflowExecution> e = Workflow.getWorkflowExecution(child);` |
| `Workflow.newPromise()` | 수동으로 `CompletablePromise<T>` 생성 (`core/.../bookingsyncsaga/TripBookingWorkflowImpl.java`) |

### 핵심 API

| 호출 | 설명 |
| --- | --- |
| `promise.get()` | 완료까지 **대기**하고 결과 반환. 실패면 unchecked exception throw |
| `promise.get(timeout, unit)` | 타임아웃 포함 대기 |
| `promise.isCompleted()` | 완료 여부 확인 (블록 안 함) |
| `promise.thenApply(fn)` | **완료 시 콜백** — 새 Promise 반환. 체이닝용 |
| `promise.handle(bifn)` | 완료 OR 실패 둘 다 처리하는 콜백 |
| `Promise.allOf(promises)` | 모든 Promise 완료까지 대기. `Promise<Void>` 반환, 하나라도 실패하면 그 예외 throw (`core/.../hello/HelloParallelActivity.java`, `.../batch/iterator/...`, `.../batch/slidingwindow/...`) |
| `Promise.anyOf(promises)` | **하나라도** 완료되면 그 결과 반환. 레이싱 패턴 핵심 (`core/.../hello/HelloCancellationScope.java`, `.../nexuscancellation/caller/HelloCallerWorkflowImpl.java`) |

### `CompletablePromise` — 수동으로 완료시키는 Promise

`Workflow.newPromise()`로 만든다. 외부 입력(Signal/Update)이나 다른 흐름이 결과를 밀어 넣어줘야 할 때 사용.

```java
// core/.../bookingsyncsaga/TripBookingWorkflowImpl.java
private final CompletablePromise<Booking> booking = Workflow.newPromise();

@WorkflowMethod
public void bookTrip(String name) {
    try {
        ...
        booking.complete(new Booking(...));           // Update 함수에서 기다리는 측 unblock
    } catch (ActivityFailure e) {
        booking.completeExceptionally(e);             // 실패 전파
        ...
    }
}

@UpdateMethod
public Booking waitForBooking() { return booking.get(); }   // Update가 결과를 받아 반환
```
→ "메인 흐름은 비동기로 진행하고, Signal/Update 핸들러는 그 결과를 기다리게" 하는 패턴.

### 사용 패턴 1 — fan-out & wait all

```java
List<Promise<String>> promises = new ArrayList<>();
for (var x : items) promises.add(Async.function(activities::process, x));
Promise.allOf(promises).get();                       // 전부 끝날 때까지
List<String> results = promises.stream().map(Promise::get).toList();  // get()은 이제 즉시 반환
```

### 사용 패턴 2 — 레이싱 (first-win)

```java
Promise<Result> winner = Promise.anyOf(results);
Result r = winner.get();                             // 하나 끝날 때까지
scope.cancel();                                      // 나머지 취소 (§14 참고)
```

### 사용 패턴 3 — Timer vs Activity 레이싱

```java
Promise<String> activityResult = Async.function(activities::doIt, input);
Promise<Void> timeout = Workflow.newTimer(Duration.ofSeconds(10));
Promise<Object> first = Promise.anyOf(activityResult, timeout);
first.get();
if (timeout.isCompleted()) throw new RuntimeException("timed out");
return activityResult.get();
```
→ 또는 §14 패턴2처럼 `timer.thenApply(r -> scope.cancel())` 조합도 가능.

### 사용 패턴 4 — `thenApply`로 콜백 체이닝

```java
Workflow.newTimer(Duration.ofSeconds(3))
    .thenApply(ignore -> { scope.cancel(); return null; });
// → core/.../hello/HelloCancellationScopeWithTimer.java, .../hello/HelloWorkflowTimer.java
```
Timer가 fire되면 scope를 cancel. `thenApply`는 **Workflow Task 안에서 호출되는 콜백**이므로 Workflow API를 자유롭게 사용 가능.

### 자주 하는 실수

1. **`CompletableFuture` 혼용** — Workflow 코드에서 `CompletableFuture.supplyAsync(...)`나 `ExecutorService.submit(...)` 쓰면 결정성 깨짐 + 리플레이 실패. 반드시 `Async` + `Promise`.
2. **Promise 안 받아서 결과도 못 쓰고 실패도 못 감지** — `Async.function(...)`의 반환을 버리면 그 작업의 예외를 못 잡는다. 적어도 `Promise.allOf(list).get()`으로 수집하거나 일부러 "fire-and-forget"임을 주석에 명시.
3. **`allOf`에서 일부 실패를 무시하고 싶은데 그냥 쓰기** — `allOf`는 첫 실패에서 throw. 전부 돌리고 결과를 모으려면 `HelloCancellationScope.java`처럼 for-loop로 각 Promise를 try/catch 하면서 `.get()`.
4. **`get()`을 Activity 안에서 호출** — Promise는 **Workflow 전용**. Activity 코드에서는 쓸 수 없다.
5. **`CompletablePromise`를 Workflow 바깥(Activity, 일반 스레드)에서 complete** — 이것도 결정성 깨짐. 반드시 Workflow 코드 안(또는 Signal/Update 핸들러 안)에서 complete.

### 샘플 레퍼런스
- fan-out + `allOf`: `core/src/main/java/io/temporal/samples/hello/HelloParallelActivity.java`, `core/src/main/java/io/temporal/samples/batch/iterator/IteratorBatchWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`
- 레이싱 + `anyOf`: `core/src/main/java/io/temporal/samples/hello/HelloCancellationScope.java`, `core/src/main/java/io/temporal/samples/nexuscancellation/caller/HelloCallerWorkflowImpl.java`
- `CompletablePromise`로 Update ↔ 메인 흐름 bridge: `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java`
- `thenApply`로 Timer 콜백: `core/src/main/java/io/temporal/samples/hello/HelloCancellationScopeWithTimer.java`, `core/src/main/java/io/temporal/samples/hello/HelloWorkflowTimer.java`
- ChildWorkflow 시작 확인(`Workflow.getWorkflowExecution`): `core/src/main/java/io/temporal/samples/asyncchild/ParentWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`
