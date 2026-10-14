# Worker 실행 모델과 Sticky Queue

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

Workflow가 "어디서, 어떤 스레드에서, 어떻게 분산되어" 실행되는지의 멘탈 모델. `core/src/main/java/io/temporal/samples/hello/HelloSignal.java` 를 레퍼런스로 짚어본다.

---

## 1. `Workflow.await()` vs `while` 루프 — 왜 `await`인가

`HelloSignal.GreetingWorkflowImpl.getGreetings()`에서:

```java
while (true) {
    Workflow.await(() -> !messageQueue.isEmpty() || exit);   // ✓
    // while (messageQueue.isEmpty() && !exit) { }           // ✗ 이렇게 하면 안 됨
    if (messageQueue.isEmpty() && exit) return receivedMessages;
    ...
}
```

**`while`로는 안 되는 이유:**
- Workflow 스레드에서 `Thread.sleep`, `Object.wait`, `CompletableFuture.get` 같은 **표준 JDK 블로킹을 쓰면 결정성이 깨짐** → replay가 재현 안 됨
- 조건만 반복 검사하는 busy-spin은 CPU만 태우고 **signal이 도착했음을 감지할 훅이 없음**

**`Workflow.await(condition)`이 하는 일:**
- Workflow 스레드를 **de-schedule** → worker는 다른 workflow task 처리 가능
- Signal / timer fire / activity 완료 등 **이벤트가 들어올 때마다 `condition` 재평가**
- `true`가 되는 순간 결정적으로 재개 → replay 시에도 동일 지점에서 통과

Workflow 코드에서 "조건이 참이 될 때까지 기다린다"는 거의 항상 `Workflow.await()` / `Workflow.sleep()`. JDK 표준 블로킹은 **쓰지 않는다**.

---

## 2. Workflow Stub은 "리모컨"일 뿐 — 스레드도, 실행도 없음

```java
GreetingWorkflow workflow1 = client.newWorkflowStub(GreetingWorkflow.class, workflowOptions);
GreetingWorkflow workflow2 = client.newWorkflowStub(GreetingWorkflow.class, workflowOptions);
```

| | Stub | Worker |
|---|---|---|
| 정체 | gRPC 호출을 감싸는 client-side **프록시** | 실제로 workflow 코드를 **실행**하는 프로세스 |
| 생성 | `client.newWorkflowStub(...)` | `WorkerFactory.newWorker(TASK_QUEUE)` |
| 스레드 | 없음 (호출 시 호출자 스레드에서 gRPC 전송) | Worker JVM 안의 workflow task executor 스레드 풀 |
| 늘리는 방법 | 메서드 호출마다 새로 만들어도 됨 | **별도 JVM 프로세스를 추가** 실행 |

**같은 `WORKFLOW_ID`로 만든 두 stub은 동일한 workflow execution의 리모컨.** `HelloSignal`에서 `workflowById`를 다시 만든 건 **"ID만 알면 signal을 보낼 수 있다"는 걸 보여주는 데모**이지, 새 workflow를 띄우는 게 아니다.

---

## 3. 다른 `WORKFLOW_ID` → 완전히 독립적인 Execution

```java
WorkflowOptions opts1 = WorkflowOptions.newBuilder().setTaskQueue(TQ).setWorkflowId("WF-1").build();
WorkflowOptions opts2 = WorkflowOptions.newBuilder().setTaskQueue(TQ).setWorkflowId("WF-2").build();
```

| 항목 | 독립 여부 |
|---|---|
| Workflow Execution (서버 instance) | ✓ 완전 별개 |
| Event History | ✓ 각자 따로 기록 |
| Workflow 상태 (`messageQueue`, `exit` 필드) | ✓ 각자 다른 `Impl` 인스턴스 |
| Signal 라우팅 | ✓ `wf1.waitForName()` → WF-1만, `wf2.waitForName()` → WF-2만 |
| Workflow 스레드 | ✓ Worker 안에서 각 execution마다 별도 workflow thread |
| 실패/재시작/타임아웃 | ✓ 서로 영향 없음 |

**공유되는 것 (물리적 리소스):**
- Worker 프로세스 / JVM / 같은 Task Queue
- Worker 스레드 풀 slot (경쟁)
- gRPC 커넥션 / `WorkflowClient`
- JVM 전역 상태 (**이래서 workflow 코드에서 `static` mutable 필드 금지**)

→ "논리적으로 완전 독립, 물리적으로 리소스 공유".

---

## 4. Worker 여러 개 — Task 분산 메커니즘

Temporal의 task 분산은 **pull 기반 (long poll)**. 서버가 push하지 않고, worker가 "일 있어?" 하고 묻는다.

```
Worker A ──long poll──> Temporal Server (Task Queue: X)
Worker B ──long poll──> Temporal Server
Worker C ──long poll──> Temporal Server
```

**Activity Task — 완전 분산:** 상태 없으니 매 호출마다 free worker 아무나 받음.

**Workflow Task — Sticky Execution:** 상태 유지 비용 때문에 캐시 친화적으로 라우팅 (아래 §6).

---

## 5. K8s에서 Worker = Pod

**1 Pod = 1 JVM = 1 Worker 프로세스**가 전형. Pod 3개 = Worker 3개.

```
Pod-1 (Worker A) ──long poll──┐
Pod-2 (Worker B) ──long poll──┼──> Temporal Server (TaskQueue: X)
Pod-3 (Worker C) ──long poll──┘
```

K8s에서 좋은 점:
- **Stateless scaling** — Workflow 상태는 서버 event history에 있음. Pod 죽어도 다른 pod가 replay로 이어받음 → HPA 자유롭게 적용 가능
- **Rolling update 안전** — 교체 중 pod가 들고 있던 task는 sticky timeout 후 fallback → 다른 pod가 받음 → 지연은 발생해도 손실은 없음
- **Pod는 cattle** — 죽여도, 늘려도, 교체해도 OK

Graceful shutdown 필수:
```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    factory.shutdown();                                   // 새 task 안 받음
    factory.awaitTermination(30, TimeUnit.SECONDS);       // 진행 중 마무리
}));
```
K8s `preStop` hook + `terminationGracePeriodSeconds` 와 맞춰야 함.

---

## 6. Sticky Queue — Replay 비용 최적화 캐시

**왜 필요한가:**
Workflow는 **event history 재생**으로 상태를 복원한다. Event 100개가 쌓인 workflow에서 매 task마다 처음부터 100개를 재생하면 비싸다. → "같은 workflow instance는 **처음 받은 worker가 메모리에 들고 있자**" = Sticky Queue.

### 6-1. 메커니즘 — 두 종류의 Queue

```
Worker A ──┬──> "OrderQueue"               (일반 queue, 다른 worker들과 공유)
           └──> "OrderQueue:A-<uuid>"      (sticky queue, A 전용)
```

Sticky queue 이름은 worker가 생성한 unique ID. 다른 worker는 모름.

### 6-2. 생명주기

```
1. 새 workflow 시작
   서버 → 일반 queue "OrderQueue"에 task 투입
   → Worker A, B, C 중 polling 중인 아무나 → Worker A 받음

2. Worker A 응답 시 StickyAttributes 포함:
   { worker_task_queue: "OrderQueue:A-xxx",
     schedule_to_start_timeout: 5s }

3. Worker A의 WorkflowCache에 instance 메모리 유지
   { "workflow-123" → GreetingWorkflowImpl + 스레드 상태 }

4. 다음 workflow task (signal, timer fire, activity 완료 등)
   서버 → 이 workflow는 sticky 설정됨 → "OrderQueue:A-xxx"에 투입
   → Worker A만 받음 → 캐시에서 꺼내 바로 처리 (replay 없음!)
```

캐시 hit 시 **마지막 처리 event 이후의 신규 event만** 재생. 전체 history 재생 불필요.

### 6-3. Fallback — Sticky가 깨지는 경우

| Case | 흐름 |
|---|---|
| **Worker A가 바쁘거나 죽음** | 5초(sticky timeout) 안에 안 가져감 → 서버가 자동으로 일반 queue 재발행 → B/C 중 하나가 받음 → cache 없으니 **full replay** → 이후 sticky가 받은 worker로 전환 |
| **캐시 가득 참** | `WorkflowCacheSize` 초과 시 LRU evict → 다음 task 때 cache miss → replay |
| **코드 non-determinism 감지** | Replay 중 결정성 깨짐 → workflow task failure → 재시도 → 캐시에서 쫓겨남 |

→ Sticky가 깨져도 **정확성에는 영향 없음, 성능만 떨어짐**.

### 6-4. 설정값

```java
// WorkerFactoryOptions (JVM 당 1개)
WorkerFactoryOptions.newBuilder()
    .setWorkflowCacheSize(600)                           // sticky 캐시 크기 (worker 전체 공유)
    .setMaxWorkflowThreadCount(600)                      // workflow 실행 스레드 풀
    .setWorkflowHostLocalTaskQueueScheduleToStartTimeout(
        Duration.ofSeconds(5))                           // sticky timeout
    .build();

// WorkerOptions (Worker 당)
WorkerOptions.newBuilder()
    .setMaxConcurrentWorkflowTaskExecutionSize(200)      // 동시 workflow task slot
    .setMaxConcurrentWorkflowTaskPollers(5)              // sticky+normal 각각의 poller 수
    .build();
```

**Trade-off:**

| 설정 | 올리면 | 내리면 |
|---|---|---|
| `WorkflowCacheSize` | Cache hit률 ↑, 메모리 ↑ | 메모리 ↓, replay 빈도 ↑ |
| `StickyScheduleToStartTimeout` | Sticky 유지률 ↑, 장애 시 지연 ↑ | 빠른 fallback, cache 효용 ↓ |

### 6-5. 관찰 지표

- `workflow_task_schedule_to_start_latency` — sticky는 ms, 일반 queue는 더 김
- `sticky_cache_hit` / `sticky_cache_miss` — 캐시 효율
- `sticky_cache_total_forced_eviction` — 캐시 밀려 쫓겨난 수. 급증 시 `WorkflowCacheSize` 키우거나 worker scale out 필요

### 6-6. K8s Rolling Update와의 상호작용

```
Pod A 종료:
  1. preStop → factory.shutdown()
  2. Worker A의 sticky queue polling 중단
  3. 서버가 "OrderQueue:A-xxx"에 쌓은 task → 5초 후 일반 queue로 재발행
  4. Pod B, C가 받아서 replay로 상태 복원 → 계속 진행
```

Sticky timeout이 **너무 길면 rolling update 지연 ↑**, **너무 짧으면 평시 cache miss ↑**.

---

## 7. 그림 요약

```
       [Client JVM]                              [Worker JVM × 3]

  stub1 ─┐                                       ┌─ Worker A ← long poll (일반 + sticky)
         ├─ gRPC → [Temporal Server] ← task queue├─ Worker B ← long poll
  stub2 ─┘                                       └─ Worker C ← long poll
         (리모컨)      (중재자)                        (실제 실행 + 캐시)
```

```
                        ┌─────────────────────────────────────┐
                        │          Temporal Server            │
                        │                                     │
                        │  TaskQueue "OrderQueue"             │
                        │    └─ 새 workflow, fallback task    │
                        │                                     │
                        │  TaskQueue "OrderQueue:A-xxx"       │  ← Worker A sticky
                        │  TaskQueue "OrderQueue:B-yyy"       │  ← Worker B sticky
                        └─────────────────────────────────────┘
                             ▲          ▲          ▲
                        [Worker A]  [Worker B]  [Worker C]
                        Cache:      Cache:      Cache:
                        wf-1 ✓      wf-2 ✓      wf-3 ✓
```

---

## 요약

- **Stub = 리모컨**, 스레드 없음. 같은 WORKFLOW_ID → 서버에 **단 하나의** execution
- **다른 WORKFLOW_ID → 독립 execution**, 상태/history/signal 모두 분리. 리소스(worker, 스레드, 커넥션)만 공유
- **Worker 분산**: pull 기반 long poll. Activity는 완전 분산, Workflow는 sticky
- **Sticky Queue** = worker별 private task queue + in-memory cache. Replay 비용 제거
- **K8s**: Pod ≈ Worker. HPA/Rolling update 안전. Graceful shutdown 필수
- Sticky가 깨져도 정확성은 유지됨 (성능만 저하)

---

## 관련 토픽

- [01 Client/Worker 셋업](01-setup.md) — `WorkerFactoryOptions` / `WorkerOptions` 상세
- [12 gRPC Retry](12-grpc-retry.md) — 전송 계층 재시도 (상호 보완 관계)
- [06 메시징](06-messaging.md) — Signal/Query/Update 메커니즘
