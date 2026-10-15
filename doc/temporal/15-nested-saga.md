# Saga 보상 패턴과 Nested Saga

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

Saga는 분산 트랜잭션에서 2PC가 불가능할 때 **각 단계마다 "되돌리는 액션(보상)"을 등록**해두고, 후속 실패 시 역순(또는 병렬)으로 실행해 상태를 되돌리는 패턴. Temporal 자바 SDK는 `io.temporal.workflow.Saga` 헬퍼로 지원하며, **CancellationScope와 조합해야 안전하게 동작**한다.

기본 사용/옵션(`setParallelCompensation`, `setContinueWithError`)은 [04 조합의 §10](04-composition.md), `CancellationScope`의 두 종류와 전파 규칙은 [05 동시성의 §14](05-concurrency.md)를 참조. 여기서는 **두 개념이 만나는 지점의 함정**과, **자식 워크플로마다 자기 Saga를 가지는 Nested Saga 패턴**을 다룬다.

## 1. 가장 많이 놓치는 함정 — `catch` 안의 `compensate()`는 반드시 **detached scope** 안에서

```java
try {
    // ... Activity, ChildWorkflow 들
} catch (ActivityFailure | ChildWorkflowFailure e) {
    // ❌ 외부에서 WorkflowStub.cancel() 들어온 상황에서 보상 Activity도 함께 cancel → 유실
    // saga.compensate();

    // ✅ root scope가 cancel 상태여도 보상이 완주하도록 detached scope
    Workflow.newDetachedCancellationScope(saga::compensate).run();
    throw e;
}
```

**왜 터지는가** — 외부에서 워크플로를 cancel하면 root scope가 cancel 상태. 이 상태에서 새 Activity를 시작하면 **즉시 cancel 신호를 받아** 보상이 유실된다. 로컬/해피 테스트에선 안 보이고, 운영에서 "stuck된 워크플로 cancel했는데 결제/재고 롤백 안 됨" 사고의 전형. 샘플: `core/src/main/java/io/temporal/samples/bookingsaga/TripBookingWorkflowImpl.java:40`.

## 2. Nested Saga — 자식도 Saga, 부모도 Saga

여행 예약을 "국내 구간(car+hotel) / 국제 구간(flight+visa) / 결제" 셋으로 나눈다고 할 때, 자식 워크플로마다 자기 Saga를 가지면 **보상 책임이 스코프별로 깔끔하게 분리**된다.

```mermaid
flowchart TD
    P["TripBookingParent<br/>(parent Saga)"]
    DL["DomesticLegWorkflow<br/>(leg Saga: car + hotel)"]
    IL["InternationalLegWorkflow<br/>(leg Saga: flight + visa)"]
    PAY["payment.charge()"]
    DU["DomesticLegCompensation<br/>Workflow.undo(result)"]
    IU["InternationalLegCompensation<br/>Workflow.undo(result)"]

    P --> DL
    P --> IL
    P --> PAY
    P -. "payment 실패 시<br/>parentSaga.compensate()<br/>(parallel)" .-> DU
    P -. .-> IU
    DL -. "자식 내부 실패 시<br/>leg Saga가 자기만 롤백" .-> DL
    IL -. .-> IL
```

### 두 가지 실패 경로

| 실패 지점 | 누가 보상? | 어떻게? |
| --- | --- | --- |
| `domestic.reserveCar` 자체 실패 | **자식의 Saga** | 자식 catch에서 `saga.compensate()` (detached) → 자식만 깨끗이 롤백 후 throw |
| 자식 성공 후 `payment` 실패 | **부모의 Saga** | `parentSaga.compensate()` → 등록해둔 `DomesticLegCompensationWorkflow.undo(...)`, `InternationalLegCompensationWorkflow.undo(...)` 자식들을 **병렬**로 시작 |

### 패턴의 네 가지 설계 원칙

1. **자식은 자기 실패를 자기 Saga로 처리** — 부모엔 "자식은 깨끗한 상태"로만 실패 전파.
2. **자식 성공 리턴값에 "보상에 필요한 ID/상태"를 담아 부모에 전달** — 보상 워크플로는 자식 코드와 상태 공유 불가.
3. **부모는 "자식 레벨 undo 워크플로"를 보상으로 등록** — 자식 성공 후 부모 단계가 실패하면 이것이 호출됨.
4. **부모 compensate도 detached scope + 보상 자식은 `ParentClosePolicy.ABANDON`** — 외부 cancel/부모 종료 모두에서 보상 완주 보장.

### 핵심 코드 스케치 (`core/src/main/java/io/temporal/samples/nestedsaga/NestedSaga.java` 참조)

**자식**: 자기 Saga로 자기 리소스 롤백 책임.

```java
public DomesticLegResult book(String name) {
    Saga saga = new Saga(new Saga.Options.Builder().build());
    String carReqId = Workflow.randomUUID().toString();
    String hotelReqId = Workflow.randomUUID().toString();
    try {
        saga.addCompensation(acts::cancelCar, carReqId);
        acts.reserveCar(carReqId, name);
        saga.addCompensation(acts::cancelHotel, hotelReqId);
        acts.bookHotel(hotelReqId, name);
        return new DomesticLegResult(carReqId, hotelReqId);   // ← 부모에 ID 전달
    } catch (ActivityFailure e) {
        Workflow.newDetachedCancellationScope(saga::compensate).run();
        throw e;
    }
}
```

**부모**: 자식 성공 시 "자식 undo 워크플로"를 보상으로 등록.

```java
Saga parentSaga = new Saga(new Saga.Options.Builder().setParallelCompensation(true).build());
try {
    DomesticLegWorkflow domestic = Workflow.newChildWorkflowStub(DomesticLegWorkflow.class);
    DomesticLegResult domesticResult = domestic.book(name);
    DomesticLegCompensationWorkflow domesticUndo =
        Workflow.newChildWorkflowStub(DomesticLegCompensationWorkflow.class,
            ChildWorkflowOptions.newBuilder()
                .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON).build());
    parentSaga.addCompensation(domesticUndo::undo, domesticResult);

    // ... intl 레그도 같은 패턴 ...

    payment.charge(name, 1000);   // ← 여기 실패하면 두 레그 전부 undo
    return new Booking(...);
} catch (ActivityFailure | ChildWorkflowFailure e) {
    Workflow.newDetachedCancellationScope(parentSaga::compensate).run();
    throw e;
}
```

## 3. 보상 Activity의 책임 — 멱등 + "원본 작업 안 됨" 케이스 처리

`addCompensation`은 **실제 Activity 호출 전에** 등록하는 게 원칙(bookingsaga 샘플 기준). 호출이 **timeout으로 실패**하면 서버 쪽은 성공했는지 알 수 없기 때문. 그래서 보상 Activity는:

- 같은 `requestId`로 조회해서 **없으면 no-op 성공**
- 이미 cancel된 상태여도 **성공 반환**
- 외부 API 호출 실패는 재시도 가능하게 `ApplicationFailure` (retryable)

## 4. 체크리스트

- ☑ `addCompensation`을 Activity 호출 **전에** 등록 — timeout 대응
- ☑ `requestId`는 **먼저 생성해서** 보상/실제 호출 둘 다에 전달 — 멱등 기반
- ☑ `catch` 블록에서 `Workflow.newDetachedCancellationScope(saga::compensate).run()`로 감싸기
- ☑ 자식 Saga의 compensate도 **detached** — 부모 cancel에도 자식 보상 완주
- ☑ 보상 자식 워크플로는 `ParentClosePolicy.ABANDON` — 부모 종료 시 보상 유실 방지
- ☑ 부모 `catch`는 **`ActivityFailure | ChildWorkflowFailure`** 둘 다 잡기
- ☑ 보상 Activity의 `RetryOptions`는 넉넉하게 — 네트워크 끊김에도 결국 성공시켜야 함
- ☑ 보상 순서가 중요하면 `parallelCompensation=false`, 아니면 `true`로 빠르게
- ☑ 중간 보상 실패해도 나머지 돌리려면 `setContinueWithError(true)` 고려

## 샘플 레퍼런스

| 패턴 | 샘플 |
| --- | --- |
| 평평한 Saga + detached scope | `core/src/main/java/io/temporal/samples/bookingsaga/TripBookingWorkflowImpl.java` |
| Local Activity + Update + Saga | `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java` |
| 자식 워크플로 + Activity + 람다 혼합 보상 | `core/src/main/java/io/temporal/samples/hello/HelloSaga.java` |
| **Nested Saga (자식 레그마다 Saga + 부모가 undo 워크플로 조립)** | `core/src/main/java/io/temporal/samples/nestedsaga/NestedSaga.java`, `core/src/test/java/io/temporal/samples/nestedsaga/NestedSagaTest.java` |
| cleanup Activity를 detached scope로 보호 | `core/src/main/java/io/temporal/samples/hello/HelloDetachedCancellationScope.java` |

→ 깊은 설명: [04 조합의 §10 Saga.Options](04-composition.md), [05 동시성의 §14 CancellationScope](05-concurrency.md)
