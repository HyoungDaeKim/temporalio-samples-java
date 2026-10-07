# 체크리스트 & 샘플 파일 레퍼런스

[← 색인으로 돌아가기](../temporal-client-worker-flow.md)

---

## 31. 셋업을 끝낼 때 확인할 체크리스트

1. **Stubs와 Client는 재사용** — 요청마다 새로 만들지 않는다. 통상 `@Singleton` 혹은 Spring Bean으로 1개만.
2. **`factory.start()`를 반드시 호출** — 호출 전에는 워커가 폴링하지 않아 워크플로가 영원히 대기 상태로 보인다.
3. **TaskQueue 이름 일치** — Worker 등록 시의 TaskQueue와 `WorkflowOptions.setTaskQueue(...)`가 반드시 같아야 함.
4. **DataConverter는 클라이언트와 워커 양쪽에 동일하게 설정** — 암호화/커스텀 변환을 쓸 때 한쪽에만 걸면 역직렬화 실패.
5. **종료 시 `factory.shutdown()` → `service.shutdown()` 순서** — 워커가 inflight 작업을 깔끔히 정리한 뒤 커넥션을 닫는다.
6. **Spring Boot 스타터 사용 시**(`springboot/`, `springboot-basic/`) 위 4개 객체는 자동 생성되고, 설정은 `application.yml`과 `TemporalOptionsConfig`(`springboot/.../customize/TemporalOptionsConfig.java`)에서 Customizer로 조정한다.

---
## 32. 샘플 파일 레퍼런스

**기본 흐름 / 커넥션**
- 기본 흐름: `core/src/main/java/io/temporal/samples/hello/HelloActivity.java`
- SSL/mTLS Stubs: `core/src/main/java/io/temporal/samples/ssl/Starter.java`

**인터셉터·DataConverter**
- 클라이언트 인터셉터 / WorkerFactory 인터셉터: `core/src/main/java/io/temporal/samples/countinterceptor/InterceptorStarter.java`
- 특정 워크플로 인터셉터 제외: `core/src/main/java/io/temporal/samples/excludefrominterceptor/RunMyWorkflows.java`
- 커스텀 DataConverter: `core/src/test/java/io/temporal/samples/payloadconverter/CryptoPayloadConverterTest.java`

**Worker Versioning**
- `WorkerOptions.setDeploymentOptions`: `core/src/main/java/io/temporal/samples/workerversioning/WorkerV1.java` 외 V1_1 / V2

**WorkflowOptions**
- Cron: `core/src/test/java/io/temporal/samples/hello/HelloCronTest.java`
- WorkflowIdReusePolicy: `core/src/main/java/io/temporal/samples/updatabletimer/DynamicSleepWorkflowStarter.java`, `core/src/test/java/io/temporal/samples/hello/HelloSignalTest.java`
- SearchAttributes / Memo: `core/src/test/java/io/temporal/samples/hello/HelloSearchAttributesTest.java`, `core/src/test/java/io/temporal/samples/listworkflows/ListWorkflowsTest.java`

**ActivityOptions**
- 4개 타임아웃 함께 설정: `core/src/main/java/io/temporal/samples/hello/HelloException.java`, `core/src/main/java/io/temporal/samples/fileprocessing/FileProcessingWorkflowImpl.java`
- Heartbeat + CancellationType: `core/src/main/java/io/temporal/samples/hello/HelloWorkflowTimer.java`
- Local vs 일반 Activity 역할 분리: `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java`

**RetryOptions**
- `MaximumAttempts(3)` LLM / 외부 API: `springai/rag/src/main/java/io/temporal/samples/springai/rag/RagWorkflowImpl.java`, `springai/basic/src/main/java/io/temporal/samples/springai/chat/ChatWorkflowImpl.java`
- `MaximumAttempts(1)` 보상(compensation) Activity: `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java`
- `DoNotRetry(FQCN)` 특정 예외 즉시 fail: `core/src/test/java/io/temporal/samples/peractivityoptions/PerActivityOptionsTest.java`, `core/src/main/java/io/temporal/samples/peractivityoptions/Starter.java`

**WorkflowImplementationOptions**
- `setFailWorkflowExceptionTypes`: `core/src/main/java/io/temporal/samples/encodefailures/Starter.java`, `core/src/main/java/io/temporal/samples/hello/HelloSignalWithStartAndWorkflowInit.java`, `core/src/test/java/io/temporal/samples/hello/HelloSignalWithStartAndWorkflowInitTest.java`
- `setActivityOptions`(Activity 타입별 옵션 매핑): `core/src/main/java/io/temporal/samples/peractivityoptions/Starter.java`
- `setNexusServiceOptions`: `core/src/test/java/io/temporal/samples/nexus/caller/CallerWorkflowTest.java`

**Saga.Options**
- `setParallelCompensation(true)` 병렬 보상: `core/src/main/java/io/temporal/samples/bookingsaga/TripBookingWorkflowImpl.java`
- 기본값(순차 보상): `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java`
- 순차 보상 + 자식 워크플로 보상: `core/src/main/java/io/temporal/samples/hello/HelloSaga.java`

**ChildWorkflowOptions**
- 비동기 자식 + `ParentClosePolicy.ABANDON`: `core/src/main/java/io/temporal/samples/asyncchild/ParentWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/asyncuntypedchild/ParentWorkflowImpl.java`
- fan-out / batch에서 continue-as-new와 함께: `core/src/main/java/io/temporal/samples/batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`
- 동기 자식 호출: `core/src/main/java/io/temporal/samples/countinterceptor/workflow/MyWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/tracing/workflow/TracingWorkflowImpl.java`
- 자식 `CancellationType`: `core/src/main/java/io/temporal/samples/hello/HelloWorkflowTimer.java`

**LocalActivityOptions**
- 기본 사용: `core/src/main/java/io/temporal/samples/hello/HelloLocalActivity.java`
- Happy path Local + 보상 일반 Activity: `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java`
- Spring Boot 혼용: `springboot/src/main/java/io/temporal/samples/springboot/customize/CustomizeWorkflowImpl.java`, `springboot/src/main/java/io/temporal/samples/springboot/update/PurchaseWorkflowImpl.java`

**CancellationScope**
- 레이싱(Activity): `core/src/main/java/io/temporal/samples/hello/HelloCancellationScope.java`
- 레이싱(Nexus): `core/src/main/java/io/temporal/samples/nexuscancellation/caller/HelloCallerWorkflowImpl.java`
- Timer로 Activity timeout 구현: `core/src/main/java/io/temporal/samples/hello/HelloCancellationScopeWithTimer.java`
- detached scope (보상 보호): `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/bookingsaga/TripBookingWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/hello/HelloDetachedCancellationScope.java`
- Signal로 scope.cancel: `core/src/test/java/io/temporal/samples/hello/HelloUpdateAndCancellationTest.java`
- Nexus handler 측 scope: `core/src/main/java/io/temporal/samples/nexuscancellation/handler/HelloHandlerWorkflowImpl.java`

**Signal**
- 기본 Signal + exit 패턴: `core/src/main/java/io/temporal/samples/hello/HelloSignal.java`
- Signal + Timer: `core/src/main/java/io/temporal/samples/hello/HelloSignalWithTimer.java`
- `signalWithStart` + `@WorkflowInit`: `core/src/main/java/io/temporal/samples/hello/HelloSignalWithStartAndWorkflowInit.java`
- 메시지 누적(Accumulator): `core/src/main/java/io/temporal/samples/hello/HelloAccumulator.java`
- 안전한 메시지 전달 패턴: `core/src/main/java/io/temporal/samples/safemessagepassing/ClusterManagerWorkflow.java`
- Signal로 Timer 갱신: `core/src/main/java/io/temporal/samples/updatabletimer/DynamicSleepWorkflow.java`
- Signal + Interceptor로 재시도: `core/src/main/java/io/temporal/samples/retryonsignalinterceptor/RetryOnSignalInterceptorListener.java`

**Query**
- 기본 Query: `core/src/main/java/io/temporal/samples/hello/HelloQuery.java`
- 상태 조회 Query 다수: `springai/rag/src/main/java/io/temporal/samples/springai/rag/RagWorkflow.java`
- Query + Signal 조합: `core/src/main/java/io/temporal/samples/packetdelivery/PacketDeliveryWorkflow.java`
- 종료된 워크플로 Query: `core/src/main/java/io/temporal/samples/listworkflows/CustomerWorkflow.java`
- Query vs Update 비교: `core/src/main/java/io/temporal/samples/hello/HelloUpdate.java`, `springboot/src/main/java/io/temporal/samples/springboot/update/PurchaseWorkflow.java`

**Update**
- 기본 Update + Validator + Rejected/Failed 처리: `core/src/main/java/io/temporal/samples/hello/HelloUpdate.java`
- Update + CancellationScope: `core/src/test/java/io/temporal/samples/hello/HelloUpdateAndCancellationTest.java`
- `updateWithStart` 조기 반환 패턴: `core/src/main/java/io/temporal/samples/earlyreturn/TransactionWorkflow.java`, `core/src/main/java/io/temporal/samples/earlyreturn/EarlyReturnClient.java`
- Spring Boot Update + Validator: `springboot/src/main/java/io/temporal/samples/springboot/update/PurchaseWorkflow.java`
- Chat 턴을 Update로: `springai/basic/src/main/java/io/temporal/samples/springai/chat/ChatWorkflow.java`
- `CompletablePromise` bridge: `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflow.java`
- 안전한 메시지 전달: `core/src/main/java/io/temporal/samples/safemessagepassing/ClusterManagerWorkflow.java`

**Timer / Workflow.sleep**
- `Workflow.sleep` 기본: `core/src/main/java/io/temporal/samples/terminateworkflow/MyWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/hello/HelloQuery.java`
- 30일 Timer + Signal 레이싱: `core/src/main/java/io/temporal/samples/sleepfordays/SleepForDaysImpl.java`
- Timer로 Activity timeout 구현: `core/src/main/java/io/temporal/samples/hello/HelloCancellationScopeWithTimer.java`
- `TimerOptions.setSummary`: `core/src/main/java/io/temporal/samples/hello/HelloWorkflowTimer.java`
- Signal + Timer 조합: `core/src/main/java/io/temporal/samples/hello/HelloSignalWithTimer.java`
- 폴링 루프에서 sleep: `core/src/main/java/io/temporal/samples/polling/periodicsequence/PeriodicPollingChildWorkflowImpl.java`
- Timer 지연 시작: `core/src/main/java/io/temporal/samples/hello/HelloDelayedStart.java`

**continue-as-new**
- `Workflow.continueAsNew` + `isEveryHandlerFinished` 안전 대기: `core/src/main/java/io/temporal/samples/safemessagepassing/ClusterManagerWorkflowImpl.java`
- `newContinueAsNewStub` + 반복 횟수 기준: `core/src/main/java/io/temporal/samples/hello/HelloPeriodic.java`
- 자연스러운 종료 (batch iterator): `core/src/main/java/io/temporal/samples/batch/iterator/IteratorBatchWorkflowImpl.java`
- Child(ABANDON) + continue-as-new fan-out: `core/src/main/java/io/temporal/samples/batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`
- Signal + Timer + continue-as-new: `core/src/main/java/io/temporal/samples/hello/HelloSignalWithTimer.java`, `core/src/main/java/io/temporal/samples/hello/HelloAccumulator.java`
- 폴링 루프에서 continue-as-new: `core/src/main/java/io/temporal/samples/polling/periodicsequence/PeriodicPollingChildWorkflowImpl.java`

**SideEffect / mutableSideEffect**
- 기본 + 결정적 대체 API 비교: `core/src/main/java/io/temporal/samples/hello/HelloSideEffect.java`
- Spring AI SideEffectTool 패턴: `springai/basic/src/main/java/io/temporal/samples/springai/chat/TimestampTools.java`, `springai/basic/src/main/java/io/temporal/samples/springai/chat/ChatWorkflowImpl.java`

**WorkflowLock (Mutex)**
- 다중 Update 핸들러 공유 상태 보호: `core/src/main/java/io/temporal/samples/safemessagepassing/ClusterManagerWorkflowImpl.java` (저장소 내 유일한 예)

**Interceptor**
- 가장 단순한 inbound/outbound 체인(카운팅): `core/src/main/java/io/temporal/samples/countinterceptor/`
- 트레이싱(OpenTracing/Jaeger): `core/src/main/java/io/temporal/samples/tracing/`
- Signal 기반 재시도 패턴: `core/src/main/java/io/temporal/samples/retryonsignalinterceptor/`
- 특정 Workflow 제외: `core/src/main/java/io/temporal/samples/excludefrominterceptor/`
- 커스텀 어노테이션 interceptor: `core/src/main/java/io/temporal/samples/customannotation/`
- Nexus + MDC 컨텍스트 전파: `core/src/main/java/io/temporal/samples/nexuscontextpropagation/`
- 리플레이 시 interceptor 동작 확인 테스트: `core/src/test/java/io/temporal/samples/interceptorreplaytest/InterceptorReplayTest.java`

**DataConverter / PayloadConverter / PayloadCodec**
- 기본 암호화 PayloadCodec: `core/src/main/java/io/temporal/samples/encryptedpayloads/CryptCodec.java`
- KMS 기반(AWS Encryption SDK): `core/src/main/java/io/temporal/samples/keymanagementencryption/awsencryptionsdk/EncryptedPayloads.java`, `KeyringCodec.java`
- Failure까지 encode: `core/src/main/java/io/temporal/samples/encodefailures/Starter.java`, `SimplePrefixPayloadCodec.java`, `core/src/test/java/io/temporal/samples/encodefailures/EncodeFailuresTest.java`
- JSON 필드별 암호화(Jackson Crypto 모듈): `core/src/test/java/io/temporal/samples/payloadconverter/CryptoPayloadConverterTest.java`
- CloudEvents 포맷 PayloadConverter: `core/src/main/java/io/temporal/samples/payloadconverter/cloudevents/CloudEventsPayloadConverter.java`, `core/src/test/java/io/temporal/samples/payloadconverter/CloudEventsPayloadConverterTest.java`
- Context propagation(MDC): `core/src/main/java/io/temporal/samples/nexuscontextpropagation/propagation/MDCContextPropagator.java`

**Schedule**
- 전체 라이프사이클(create → trigger → update → unpause → describe → delete): `core/src/main/java/io/temporal/samples/hello/HelloSchedules.java`

**Replay**
- 결정적/비결정적 리플레이 비교: `core/src/test/java/io/temporal/samples/hello/HelloActivityReplayTest.java`
- Interceptor 리플레이 동작 검증: `core/src/test/java/io/temporal/samples/interceptorreplaytest/InterceptorReplayTest.java`

**TestWorkflowRule / Extension / Environment**
- JUnit 4 Rule + time-skip: `core/src/test/java/io/temporal/samples/sleepfordays/SleepForDaysTest.java`
- JUnit 5 Extension + 파라미터 주입: `core/src/test/java/io/temporal/samples/sleepfordays/SleepForDaysJUnit5Test.java`
- Activity mock: `core/src/test/java/io/temporal/samples/hello/HelloCronTest.java`
- Nexus mock: `core/src/test/java/io/temporal/samples/nexus/caller/NexusServiceMockTest.java`, `CallerWorkflowMockTest.java`
- `setWorkflowClientOptions`로 DataConverter 주입: `core/src/test/java/io/temporal/samples/payloadconverter/CryptoPayloadConverterTest.java`
- `setWorkerFactoryOptions`로 Interceptor 주입: `core/src/test/java/io/temporal/samples/tracing/TracingTest.java`
- Nexus endpoint 바꿔치기: `core/src/test/java/io/temporal/samples/nexus/caller/CallerWorkflowTest.java`
- Spring Boot 통합: `springboot/src/test/java/io/temporal/samples/springboot/HelloSampleTest.java`, `HelloSampleTestMockedActivity.java`

**Workflow.getVersion (Patching)**
- 기본 사용 + custom search attribute: `core/src/main/java/io/temporal/samples/customchangeversion/CustomChangeVersionWorkflowImpl.java`
- Auto-upgrade와 결합: `core/src/main/java/io/temporal/samples/workerversioning/AutoUpgradingWorkflowV1bImpl.java`

**Worker Versioning**
- Worker 설정(1.0 / 1.1 / 2.0): `core/src/main/java/io/temporal/samples/workerversioning/WorkerV1.java`, `WorkerV1_1.java`, `WorkerV2.java`
- Pinned Workflow: `core/src/main/java/io/temporal/samples/workerversioning/PinnedWorkflowV1Impl.java`, `PinnedWorkflowV2Impl.java`
- Auto-upgrade Workflow: `core/src/main/java/io/temporal/samples/workerversioning/AutoUpgradingWorkflowV1Impl.java`, `AutoUpgradingWorkflowV1bImpl.java`
- Starter + current version 승격: `core/src/main/java/io/temporal/samples/workerversioning/Starter.java`

**Async / Promise**
- fan-out + `Promise.allOf`: `core/src/main/java/io/temporal/samples/hello/HelloParallelActivity.java`, `core/src/main/java/io/temporal/samples/batch/iterator/IteratorBatchWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`
- 레이싱 + `Promise.anyOf`: `core/src/main/java/io/temporal/samples/hello/HelloCancellationScope.java`, `core/src/main/java/io/temporal/samples/nexuscancellation/caller/HelloCallerWorkflowImpl.java`
- `Async.function` 비동기 ChildWorkflow: `core/src/main/java/io/temporal/samples/hello/HelloChild.java`, `core/src/main/java/io/temporal/samples/asyncchild/ParentWorkflowImpl.java`
- `Async.procedure` (void 반환): `core/src/main/java/io/temporal/samples/batch/iterator/IteratorBatchWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/batch/slidingwindow/SlidingWindowBatchWorkflowImpl.java`
- `CompletablePromise` + Update bridge: `core/src/main/java/io/temporal/samples/bookingsyncsaga/TripBookingWorkflowImpl.java`
- `thenApply` 콜백: `core/src/main/java/io/temporal/samples/hello/HelloCancellationScopeWithTimer.java`, `core/src/main/java/io/temporal/samples/hello/HelloWorkflowTimer.java`

**NexusServiceOptions / NexusOperationOptions**
- 서비스 호출 + Cancellation 조합: `core/src/main/java/io/temporal/samples/nexuscancellation/caller/HelloCallerWorkflowImpl.java`
- 다중 인자 operation: `core/src/main/java/io/temporal/samples/nexusmultipleargs/caller/HelloCallerWorkflowImpl.java`, `core/src/main/java/io/temporal/samples/nexusmultipleargs/caller/EchoCallerWorkflowImpl.java`
- 워커 등록 시 endpoint 주입(`WorkflowImplementationOptions.setNexusServiceOptions`): `core/src/main/java/io/temporal/samples/nexuscancellation/caller/CallerWorker.java`, `core/src/main/java/io/temporal/samples/nexusmultipleargs/caller/CallerWorker.java`
- 테스트에서 endpoint 바꿔치기: `core/src/test/java/io/temporal/samples/nexus/caller/CallerWorkflowTest.java`

**Spring Boot 자동 구성**
- Spring Boot Customizer: `springboot/src/main/java/io/temporal/samples/springboot/customize/TemporalOptionsConfig.java`
