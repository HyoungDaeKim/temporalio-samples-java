# 버저닝 — getVersion / Worker Versioning

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 29. `Workflow.getVersion` — 코드 레벨 Patching (결정성 유지하며 로직 변경)

### 역할
이미 실행 중인(혹은 과거에 실행된) Workflow가 있는 상태에서 **Workflow 코드를 변경**할 때, 리플레이 결정성을 깨지 않고 **구/신 경로 분기**를 안전하게 추가하는 메커니즘. 결정성을 깨는 변경(§27 Replay 참고)을 꼭 해야 할 때의 공식 해결책.

### 왜 필요한가

Workflow 코드가 Activity 호출을 하나 **추가/삭제/순서 변경**하면, 과거 히스토리를 리플레이할 때 **커맨드 순서가 안 맞아** `NonDeterministicException`이 발생한다. `getVersion`은 **"이 변경이 적용된 시점"을 히스토리에 marker로 적재**해, 리플레이 때 해당 Workflow가 어느 버전으로 시작했는지 알 수 있게 한다.

### 기본 사용 (`core/.../customchangeversion/CustomChangeVersionWorkflowImpl.java`)

```java
@Override
public String run(String input) {
    String result = activities.customOne(input);

    // 과거 실행: customTwo 호출 없이 끝났음
    // 새로 추가하되, 과거 실행도 안전하게 복구되게 하려면:
    int version = Workflow.getVersion("add-v2-activity-change", Workflow.DEFAULT_VERSION, 1);
    if (version == 1) {
        result += activities.customTwo(input);        // 새 Workflow만 이 경로
    }
    // 과거 Workflow는 version == DEFAULT_VERSION(-1)이므로 분기 안 탐 → 리플레이 안전

    // 또 다른 변경을 나중에 추가
    version = Workflow.getVersion("add-v3-activity-change", Workflow.DEFAULT_VERSION, 1);
    if (version == 1) {
        result += activities.customThree(input);
    }

    return result;
}
```

### `getVersion` 시그니처

```java
int getVersion(String changeId, int minSupported, int maxSupported);
```

| 파라미터 | 의미 |
| --- | --- |
| `changeId` | 변경을 구분하는 **고유 ID**. 코드 변경마다 다른 ID 사용 |
| `minSupported` | 지원하는 **최소 버전**. `Workflow.DEFAULT_VERSION`(-1)이면 "이 변경 전부터 있던 Workflow도 지원" |
| `maxSupported` | 지원하는 **최대 버전**. 보통 새로 추가한 버전 번호 |

### 동작 원리

- **첫 호출(새 Workflow)**: `maxSupported`를 반환하고 `MarkerRecorded` 이벤트로 히스토리에 적재.
- **리플레이**: 히스토리의 marker에서 **그때 적힌 버전**을 돌려줌. 코드가 바뀌어도 과거 Workflow는 과거 경로로.
- **marker 없이 지나간 과거 Workflow**: `Workflow.DEFAULT_VERSION`을 반환. `minSupported`가 `DEFAULT_VERSION`이면 "분기 안 타는" 경로로 안전하게 진행.

### 버전 하나씩 추가하는 전형 흐름

```java
// v0 (초기)
activities.doStep();

// v1 추가 — "N번째 변경을 추가"
int v = Workflow.getVersion("add-validation", Workflow.DEFAULT_VERSION, 1);
if (v >= 1) activities.validate();
activities.doStep();

// v2 추가 — 같은 changeId에 또 다른 변경
int v = Workflow.getVersion("add-validation", Workflow.DEFAULT_VERSION, 2);
if (v == 1) activities.validate();
else if (v == 2) activities.validateV2();
activities.doStep();

// 오래되어 모든 과거 Workflow가 완료됐으면 — minSupported 올려서 분기 단순화
int v = Workflow.getVersion("add-validation", 1, 2);  // DEFAULT_VERSION 지원 중단
if (v == 1) activities.validate();
else activities.validateV2();
activities.doStep();

// 그 다음엔 getVersion 자체를 제거 가능 (v == 2만 남으면)
activities.validateV2();
activities.doStep();
```

### 커스텀 Search Attribute로 버전 분포 추적 (`core/.../customchangeversion/CustomChangeVersionWorkflowImpl.java`)

`getVersion` 호출 결과를 **커스텀 search attribute**로 upsert하면 "어느 버전의 Workflow가 몇 개 돌고 있는지" 운영 조회 가능:

```java
Workflow.upsertTypedSearchAttributes(
    CUSTOM_CHANGE_VERSION.valueUnset(),
    CUSTOM_CHANGE_VERSION.valueSet("add-v2-activity-change-1"));   // changeId-version
```
→ UI/List API에서 `CustomChangeVersion = "add-v2-activity-change-1"`로 검색해 어느 Workflow가 신규 경로를 탔는지 볼 수 있음. (SDK에서 `TemporalChangeVersion`을 자동 세팅할 때까지의 임시 방편으로 샘플에 문서화됨.)

### 변경 유형별 대응

| 변경 | 안전한가? | 방법 |
| --- | --- | --- |
| **Activity 호출 추가/삭제** | X | `getVersion` 분기 |
| **Activity 호출 순서 변경** | X | `getVersion` 분기 (복잡함, 가능하면 피하기) |
| **Timer duration 변경** | X | `getVersion` 분기 |
| **분기 조건 반전** | X | `getVersion` 분기 |
| **Activity 결과를 가지고 하는 로컬 계산 로직 변경** | O (Activity 호출 자체가 안 바뀌면) | 그냥 변경 |
| **로그 메시지 변경, 변수명 변경** | O | 그냥 변경 |
| **`ActivityOptions` 변경 (타임아웃 등)** | O | 그냥 변경 — 다음 Activity 호출부터 적용 |
| **새 `@SignalMethod` / `@QueryMethod` / `@UpdateMethod` 추가** | O | 그냥 추가 |
| **기존 `@SignalMethod` 삭제** | X (실행 중 Workflow에 들어온 Signal이 핸들러 못 찾음) | 핸들러는 유지하고 내부 로직만 변경 |

### 자주 하는 실수

1. **`changeId`를 매번 다르게 지음** — "copy-paste해서 같은 로직인데 id가 달라지는" 경우 리플레이 시 히스토리 marker랑 코드가 안 맞음. 한 변경에는 **고유한 id 하나**.
2. **`getVersion`을 `if` 바깥에서 호출 안 함** — 호출 자체가 조건 안에 들어가면 과거 Workflow가 그 조건을 탔는지에 따라 marker가 있을 수도 없을 수도. 보통 unconditional 호출.
3. **`minSupported`를 너무 빨리 올림** — 아직 과거 Workflow가 안 끝났는데 `minSupported=1`로 올리면 `UnsupportedVersion`이 터진다. **운영 환경의 가장 오래된 Run이 완료될 때까지** `DEFAULT_VERSION` 지원.
4. **Replay 테스트 안 하고 `getVersion` 분기 추가** — 코드만 보고 안전해 보여도 실제 리플레이로 확인해야 함. §27 Replay와 함께 사용.
5. **branch 안에서 다시 `getVersion`** — 중첩되면 복잡도 폭증. 새 changeId로 flat하게.
6. **long-running Workflow에서 getVersion만 계속 쌓임** — Workflow가 수년 돌면 patch가 수십 개 쌓인다. `continue-as-new`로 **새 Run 시작 시점에 patch를 "정리"**하는 전략 (`minSupported` 올리기).

### 샘플 레퍼런스
- 기본 `getVersion` + custom search attribute 패턴: `core/src/main/java/io/temporal/samples/customchangeversion/CustomChangeVersionWorkflowImpl.java`
- Worker Versioning과 조합한 호환 변경: `core/src/main/java/io/temporal/samples/workerversioning/AutoUpgradingWorkflowV1bImpl.java` (§30 참고)

---
## 30. Worker Versioning — 서버가 관리하는 배포 버전 (Deployment)

### 역할
`getVersion`(§29)이 **코드 안에서** 호환성을 다룬다면, **Worker Versioning**은 **서버가 "어느 Worker가 어느 버전인지"를 알고** 각 Workflow Execution을 올바른 버전의 Worker로 라우팅하는 상위 메커니즘. 신규 Worker 배포 시 기존 Workflow가 깨지지 않도록 서버가 **버전별 라우팅**을 자동 처리한다.

### 두 가지 버전 전략 — Pinned vs Auto-upgrade

| 전략 | 설명 | 샘플 |
| --- | --- | --- |
| **Pinned** | Workflow가 **시작된 버전에 묶여** 완료까지 그 버전으로만 실행. 오래 걸리는 Workflow에 안정적 | `PinnedWorkflowV1Impl.java`, `PinnedWorkflowV2Impl.java` |
| **Auto-upgrade** | **현재(current) 버전으로 자동 이동**. 새 버전 배포 시 진행 중인 Workflow가 다음 Workflow Task부터 새 버전 Worker에 라우팅 — 호환되는 변경만 허용 | `AutoUpgradingWorkflowV1Impl.java`, `AutoUpgradingWorkflowV1bImpl.java` |

**Workflow Type이 전략을 결정** — 메서드에 `@WorkflowVersioningBehavior(...)` 어노테이션:

```java
@Override
@WorkflowVersioningBehavior(VersioningBehavior.AUTO_UPGRADE)
public void run() { ... }
```

### Worker 쪽 설정 (`core/.../workerversioning/WorkerV1.java`)

```java
// 1) 배포 버전 정의 — deployment 이름 + buildId
WorkerDeploymentVersion version = new WorkerDeploymentVersion(
    Starter.DEPLOYMENT_NAME,     // "my-deployment"
    "1.0");                       // buildId

// 2) WorkerOptions에 주입
WorkerDeploymentOptions deploymentOptions = WorkerDeploymentOptions.newBuilder()
    .setUseVersioning(true)
    .setVersion(version)
    .build();

WorkerOptions workerOptions = WorkerOptions.newBuilder()
    .setDeploymentOptions(deploymentOptions)
    .build();

Worker worker = factory.newWorker(TASK_QUEUE, workerOptions);
worker.registerWorkflowImplementationTypes(
    AutoUpgradingWorkflowV1Impl.class, PinnedWorkflowV1Impl.class);
```

→ 같은 TaskQueue에 **버전 1.0 / 1.1 / 2.0 Worker를 동시에 띄워**도 서버가 알아서 Workflow를 적절한 Worker로 보냄.

### 클라이언트는 버전 불문 (`core/.../workerversioning/Starter.java`)

```java
// 클라이언트는 Workflow Type만 지정 — 버전 정보 없음
WorkflowStub stub = client.newUntypedWorkflowStub(
    "AutoUpgradingWorkflow",                       // 버전 번호 없음!
    WorkflowOptions.newBuilder()
        .setWorkflowId(id)
        .setTaskQueue(TASK_QUEUE)
        .build());
stub.start();
```
→ 어느 버전으로 시작할지는 **서버의 "current version" 설정**이 결정. 코드 재배포만으로 바뀌지 않고 **명시적 승격 API**를 호출해야 함.

### 서버에 "current version" 설정하기 (`Starter.java`)

```java
SetWorkerDeploymentCurrentVersionRequest req =
    SetWorkerDeploymentCurrentVersionRequest.newBuilder()
        .setNamespace(ns)
        .setDeploymentName(DEPLOYMENT_NAME)
        .setBuildId("1.1")                 // 이제부터 1.1이 current
        .build();
service.blockingStub().setWorkerDeploymentCurrentVersion(req);
```
- 신규 Workflow와 Auto-upgrade Workflow의 **다음 Workflow Task**는 이 시점부터 "1.1" Worker로 라우팅.
- Pinned Workflow는 **원래 버전**(1.0)에서 계속 실행.

### 샘플의 전체 흐름 (`Starter.java`)

```
1. v1.0 Worker 뜸 → current=1.0 설정
2. AutoUpgradingWorkflow, PinnedWorkflow 시작 → 둘 다 1.0에서 시작
3. 두 Workflow 모두 signal 몇 번 → 1.0에서 처리됨
4. v1.1 Worker 뜸 → current=1.1 설정
5. 더 signal → Auto는 1.1로 자동 이동, Pinned는 1.0에 머묾
6. v2.0 Worker 뜸 → current=2.0 설정
7. 새 Pinned Workflow 시작 → 2.0에서 시작 (기존 Pinned는 1.0 유지)
8. 모든 Workflow conclude
```
→ UI에서 "Pinned는 끝까지 1.0, Auto는 1.0→1.1 이동, 신규는 2.0"이 보임.

### 호환 변경 vs 비호환 변경 (Auto-upgrade에서만 중요)

`AutoUpgradingWorkflowV1bImpl.java` 주석: Auto-upgrade에서 **호환 변경**은:
- 로그 메시지 변경
- Activity 호출 결과를 가지고 하는 로컬 계산 로직 변경
- **`Workflow.getVersion`을 사용한 분기 추가** (§29)

**비호환 변경**(Pinned로 전환 필요):
- Activity 호출 추가/삭제/순서 변경 (단, `getVersion`으로 감싸면 호환 가능)
- Workflow 메서드 시그니처 변경

→ **비호환 변경이면 `@WorkflowVersioningBehavior(PINNED)`로 선언**하거나, 새 Workflow Type을 만들어 Starter가 새 이름으로 시작하게.

### 레거시 Build ID 기반 Versioning은?

이전엔 `WorkerOptions.setBuildId(...)` + 서버의 "compatible build IDs" API로 처리했는데, **Deployment 기반 Worker Versioning**이 상위 호환. 신규 설계는 Deployment 쪽을 권장.

### Pinned vs Auto-upgrade vs `getVersion` 선택 가이드

| 상황 | 선택 |
| --- | --- |
| 모든 변경이 호환 변경 (로그, 계산) | **Auto-upgrade** 전부 |
| 특정 변경이 비호환인데 Workflow가 짧게 끝남 | Pinned로 선언 + 새 Workflow Type |
| Workflow가 길고 비호환 변경이 자주 필요 | **`getVersion` 분기** + Auto-upgrade |
| 과거 Run 완전 유지 필요 (법적/감사 이유) | **Pinned** 명시 |

### 자주 하는 실수

1. **버전 없이 Worker 띄우고 설정 바꿈** — `setUseVersioning(true)`가 꺼져 있으면 모든 Workflow가 이 Worker로도 라우팅. 혼용 시 결정성 깨짐 가능. **배포 시작 시점에 결정** 필수.
2. **current version 설정 안 하고 신규 Worker만 띄움** — 새 Workflow가 어디로 갈지 결정 안 됨. `SetWorkerDeploymentCurrentVersion` 반드시 호출.
3. **Auto-upgrade인데 비호환 변경** — 다음 Workflow Task부터 `NonDeterministicException`. 비호환이면 Pinned 전환 또는 `getVersion`.
4. **Pinned Workflow가 끝나기 전에 구 버전 Worker 종료** — Pinned는 **원래 Worker 버전만** 처리 가능. 구 Worker가 사라지면 영구 대기. 완료까지 유지 필수.
5. **`@WorkflowVersioningBehavior` 안 붙임** — 기본 전략이 설정에 따라 다름. 명시적으로 선언.
6. **Deployment 이름이 바뀜** — Deployment는 **버전의 그룹**. 이름 바꾸면 서버 입장에서 완전히 새 deployment. 안정적 이름 유지.

### 샘플 레퍼런스
- Worker 셋업 (`WorkerDeploymentOptions` + buildId): `core/src/main/java/io/temporal/samples/workerversioning/WorkerV1.java`, `WorkerV1_1.java`, `WorkerV2.java`
- Auto-upgrade Workflow (호환 변경 + `getVersion`): `core/src/main/java/io/temporal/samples/workerversioning/AutoUpgradingWorkflowV1Impl.java`, `AutoUpgradingWorkflowV1bImpl.java`
- Pinned Workflow: `core/src/main/java/io/temporal/samples/workerversioning/PinnedWorkflowV1Impl.java`, `PinnedWorkflowV2Impl.java`
- Starter + current version 승격 흐름: `core/src/main/java/io/temporal/samples/workerversioning/Starter.java`
- 호환 변경이 적용된 Workflow 예시: `core/src/main/java/io/temporal/samples/workerversioning/AutoUpgradingWorkflowV1bImpl.java` (주석에 호환 변경 목록 명시)
