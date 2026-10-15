package io.temporal.samples.nestedsaga;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityOptions;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.RetryOptions;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.failure.ChildWorkflowFailure;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Saga;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

/**
 * Nested Saga sample. A parent workflow orchestrates two child "leg" workflows (Domestic:
 * car+hotel, International: flight+visa) plus a parent-level payment step.
 *
 * <p>Each leg owns its own {@link Saga} so a failure inside the leg is rolled back internally and
 * the parent only sees "all-or-nothing" results. When the parent-level step after a successful leg
 * fails (payment, in this sample), the parent fires per-leg compensation child workflows that undo
 * work the leg has already committed.
 */
public class NestedSaga {

  static final String TASK_QUEUE = "NestedSagaTaskQueue";
  static final String WORKFLOW_ID = "NestedSagaTripBookingWorkflow";

  // =========================================================================
  // Result DTOs - carry the request IDs the compensation workflows need.
  // =========================================================================

  public static final class DomesticLegResult {
    private String carReqId;
    private String hotelReqId;

    public DomesticLegResult() {}

    public DomesticLegResult(String carReqId, String hotelReqId) {
      this.carReqId = carReqId;
      this.hotelReqId = hotelReqId;
    }

    public String getCarReqId() {
      return carReqId;
    }

    public String getHotelReqId() {
      return hotelReqId;
    }

    @Override
    public String toString() {
      return "DomesticLegResult{car=" + carReqId + ", hotel=" + hotelReqId + '}';
    }
  }

  public static final class InternationalLegResult {
    private String flightReqId;
    private String visaReqId;

    public InternationalLegResult() {}

    public InternationalLegResult(String flightReqId, String visaReqId) {
      this.flightReqId = flightReqId;
      this.visaReqId = visaReqId;
    }

    public String getFlightReqId() {
      return flightReqId;
    }

    public String getVisaReqId() {
      return visaReqId;
    }

    @Override
    public String toString() {
      return "InternationalLegResult{flight=" + flightReqId + ", visa=" + visaReqId + '}';
    }
  }

  public static final class Booking {
    private DomesticLegResult domestic;
    private InternationalLegResult international;

    public Booking() {}

    public Booking(DomesticLegResult domestic, InternationalLegResult international) {
      this.domestic = domestic;
      this.international = international;
    }

    public DomesticLegResult getDomestic() {
      return domestic;
    }

    public InternationalLegResult getInternational() {
      return international;
    }

    @Override
    public String toString() {
      return "Booking{" + domestic + ", " + international + '}';
    }
  }

  // =========================================================================
  // Activities. The impl prints what it is doing so the demo output tells a story.
  // =========================================================================

  @ActivityInterface
  public interface BookingActivities {
    String reserveCar(String reqId, String name);

    String bookHotel(String reqId, String name);

    String bookFlight(String reqId, String name);

    String issueVisa(String reqId, String name);

    void charge(String name, int amount);

    void cancelCar(String reqId);

    void cancelHotel(String reqId);

    void cancelFlight(String reqId);

    void cancelVisa(String reqId);
  }

  public static class BookingActivitiesImpl implements BookingActivities {
    @Override
    public String reserveCar(String reqId, String name) {
      System.out.println("[act] reserveCar  req=" + reqId + " name=" + name);
      return UUID.randomUUID().toString();
    }

    @Override
    public String bookHotel(String reqId, String name) {
      System.out.println("[act] bookHotel   req=" + reqId + " name=" + name);
      return UUID.randomUUID().toString();
    }

    @Override
    public String bookFlight(String reqId, String name) {
      System.out.println("[act] bookFlight  req=" + reqId + " name=" + name);
      return UUID.randomUUID().toString();
    }

    @Override
    public String issueVisa(String reqId, String name) {
      System.out.println("[act] issueVisa   req=" + reqId + " name=" + name);
      return UUID.randomUUID().toString();
    }

    /**
     * Payment is wired to fail so the demo exercises the parent-level compensation path. Real
     * idempotent compensation activities would look at the request state and no-op if the original
     * operation never happened.
     */
    @Override
    public void charge(String name, int amount) {
      System.out.println("[act] charge      name=" + name + " amount=" + amount + " -> FAIL");
      throw ApplicationFailure.newNonRetryableFailure("payment declined", "paymentFailure");
    }

    @Override
    public void cancelCar(String reqId) {
      System.out.println("[act] cancelCar   req=" + reqId);
    }

    @Override
    public void cancelHotel(String reqId) {
      System.out.println("[act] cancelHotel req=" + reqId);
    }

    @Override
    public void cancelFlight(String reqId) {
      System.out.println("[act] cancelFlight req=" + reqId);
    }

    @Override
    public void cancelVisa(String reqId) {
      System.out.println("[act] cancelVisa  req=" + reqId);
    }
  }

  // =========================================================================
  // Domestic leg: self-contained Saga for car + hotel.
  // =========================================================================

  @WorkflowInterface
  public interface DomesticLegWorkflow {
    @WorkflowMethod
    DomesticLegResult book(String name);
  }

  public static class DomesticLegWorkflowImpl implements DomesticLegWorkflow {
    private final BookingActivities acts =
        Workflow.newActivityStub(
            BookingActivities.class,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(10))
                .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
                .build());

    @Override
    public DomesticLegResult book(String name) {
      Saga saga = new Saga(new Saga.Options.Builder().build());
      String carReqId = Workflow.randomUUID().toString();
      String hotelReqId = Workflow.randomUUID().toString();
      try {
        saga.addCompensation(acts::cancelCar, carReqId);
        acts.reserveCar(carReqId, name);

        saga.addCompensation(acts::cancelHotel, hotelReqId);
        acts.bookHotel(hotelReqId, name);

        return new DomesticLegResult(carReqId, hotelReqId);
      } catch (ActivityFailure e) {
        // detached so a parent-level cancel can't cut compensation off mid-flight.
        Workflow.newDetachedCancellationScope(saga::compensate).run();
        throw e;
      }
    }
  }

  /**
   * Separate workflow invoked by the parent to undo a Domestic leg that already returned
   * successfully. Keeping this in its own workflow lets the undo re-attempt with its own retry
   * budget independent of the original leg.
   */
  @WorkflowInterface
  public interface DomesticLegCompensationWorkflow {
    @WorkflowMethod
    void undo(DomesticLegResult result);
  }

  public static class DomesticLegCompensationWorkflowImpl
      implements DomesticLegCompensationWorkflow {
    private final BookingActivities acts =
        Workflow.newActivityStub(
            BookingActivities.class,
            ActivityOptions.newBuilder()
                // Compensation runs against possibly-flaky external systems and leaking a
                // reservation is worse than retrying for a while, so budget is generous.
                .setStartToCloseTimeout(Duration.ofMinutes(5))
                .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(10).build())
                .build());

    @Override
    public void undo(DomesticLegResult r) {
      // continueWithError so one failing cancel does not leak the sibling reservation.
      Saga saga = new Saga(new Saga.Options.Builder().setContinueWithError(true).build());
      saga.addCompensation(acts::cancelCar, r.getCarReqId());
      saga.addCompensation(acts::cancelHotel, r.getHotelReqId());
      saga.compensate();
    }
  }

  // =========================================================================
  // International leg: same shape as Domestic, with flight + visa.
  // =========================================================================

  @WorkflowInterface
  public interface InternationalLegWorkflow {
    @WorkflowMethod
    InternationalLegResult book(String name);
  }

  public static class InternationalLegWorkflowImpl implements InternationalLegWorkflow {
    private final BookingActivities acts =
        Workflow.newActivityStub(
            BookingActivities.class,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(10))
                .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
                .build());

    @Override
    public InternationalLegResult book(String name) {
      Saga saga = new Saga(new Saga.Options.Builder().build());
      String flightReqId = Workflow.randomUUID().toString();
      String visaReqId = Workflow.randomUUID().toString();
      try {
        saga.addCompensation(acts::cancelFlight, flightReqId);
        acts.bookFlight(flightReqId, name);

        saga.addCompensation(acts::cancelVisa, visaReqId);
        acts.issueVisa(visaReqId, name);

        return new InternationalLegResult(flightReqId, visaReqId);
      } catch (ActivityFailure e) {
        Workflow.newDetachedCancellationScope(saga::compensate).run();
        throw e;
      }
    }
  }

  @WorkflowInterface
  public interface InternationalLegCompensationWorkflow {
    @WorkflowMethod
    void undo(InternationalLegResult result);
  }

  public static class InternationalLegCompensationWorkflowImpl
      implements InternationalLegCompensationWorkflow {
    private final BookingActivities acts =
        Workflow.newActivityStub(
            BookingActivities.class,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(5))
                .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(10).build())
                .build());

    @Override
    public void undo(InternationalLegResult r) {
      Saga saga = new Saga(new Saga.Options.Builder().setContinueWithError(true).build());
      saga.addCompensation(acts::cancelFlight, r.getFlightReqId());
      saga.addCompensation(acts::cancelVisa, r.getVisaReqId());
      saga.compensate();
    }
  }

  // =========================================================================
  // Parent orchestrator.
  // =========================================================================

  @WorkflowInterface
  public interface TripBookingParentWorkflow {
    @WorkflowMethod
    Booking bookTrip(String name);
  }

  public static class TripBookingParentWorkflowImpl implements TripBookingParentWorkflow {

    private final BookingActivities payment =
        Workflow.newActivityStub(
            BookingActivities.class,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(10))
                .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
                .build());

    // ABANDON: compensation children survive if the parent is terminated unexpectedly.
    // Normally .run() blocks until they finish anyway; this is defense in depth.
    private final ChildWorkflowOptions compensationChildOptions =
        ChildWorkflowOptions.newBuilder()
            .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
            .build();

    @Override
    public Booking bookTrip(String name) {
      Saga parentSaga = new Saga(new Saga.Options.Builder().setParallelCompensation(true).build());
      try {
        DomesticLegWorkflow domestic = Workflow.newChildWorkflowStub(DomesticLegWorkflow.class);
        DomesticLegResult domesticResult = domestic.book(name);
        DomesticLegCompensationWorkflow domesticUndo =
            Workflow.newChildWorkflowStub(
                DomesticLegCompensationWorkflow.class, compensationChildOptions);
        parentSaga.addCompensation(domesticUndo::undo, domesticResult);

        InternationalLegWorkflow intl =
            Workflow.newChildWorkflowStub(InternationalLegWorkflow.class);
        InternationalLegResult intlResult = intl.book(name);
        InternationalLegCompensationWorkflow intlUndo =
            Workflow.newChildWorkflowStub(
                InternationalLegCompensationWorkflow.class, compensationChildOptions);
        parentSaga.addCompensation(intlUndo::undo, intlResult);

        payment.charge(name, 1000);

        return new Booking(domesticResult, intlResult);
      } catch (ActivityFailure | ChildWorkflowFailure e) {
        Workflow.newDetachedCancellationScope(parentSaga::compensate).run();
        throw e;
      }
    }
  }

  // =========================================================================
  // Starter.
  // =========================================================================

  public static void main(String[] args) {
    ClientConfigProfile profile;
    try {
      profile = ClientConfigProfile.load();
    } catch (IOException e) {
      throw new RuntimeException("Failed to load client configuration", e);
    }

    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions());
    WorkflowClient client = WorkflowClient.newInstance(service, profile.toWorkflowClientOptions());
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(
        TripBookingParentWorkflowImpl.class,
        DomesticLegWorkflowImpl.class,
        DomesticLegCompensationWorkflowImpl.class,
        InternationalLegWorkflowImpl.class,
        InternationalLegCompensationWorkflowImpl.class);
    worker.registerActivitiesImplementations(new BookingActivitiesImpl());

    factory.start();

    TripBookingParentWorkflow workflow =
        client.newWorkflowStub(
            TripBookingParentWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(WORKFLOW_ID)
                .setTaskQueue(TASK_QUEUE)
                .build());

    try {
      Booking b = workflow.bookTrip("Alice");
      System.out.println("Booked: " + b);
    } catch (RuntimeException e) {
      System.out.println("Booking failed as expected: " + e.getMessage());
    }
    System.exit(0);
  }
}
