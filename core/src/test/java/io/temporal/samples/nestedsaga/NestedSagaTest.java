package io.temporal.samples.nestedsaga;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.temporal.client.WorkflowException;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.TestWorkflowExtension;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;

public class NestedSagaTest {

  @RegisterExtension
  public static final TestWorkflowExtension testWorkflowExtension =
      TestWorkflowExtension.newBuilder()
          .setWorkflowTypes(
              NestedSaga.TripBookingParentWorkflowImpl.class,
              NestedSaga.DomesticLegWorkflowImpl.class,
              NestedSaga.DomesticLegCompensationWorkflowImpl.class,
              NestedSaga.InternationalLegWorkflowImpl.class,
              NestedSaga.InternationalLegCompensationWorkflowImpl.class)
          .setDoNotStart(true)
          .build();

  /**
   * Both legs succeed, then the parent-level payment fails. The parent must fire both leg
   * compensation child workflows, which in turn must cancel every reservation that was made.
   */
  @Test
  public void bothLegsAreCompensatedWhenParentPaymentFails(
      TestWorkflowEnvironment testEnv,
      Worker worker,
      NestedSaga.TripBookingParentWorkflow workflow) {
    NestedSaga.BookingActivities activities = mock(NestedSaga.BookingActivities.class);

    ArgumentCaptor<String> carReqIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> hotelReqIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> flightReqIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> visaReqIdCaptor = ArgumentCaptor.forClass(String.class);

    when(activities.reserveCar(carReqIdCaptor.capture(), eq("Alice"))).thenReturn("car-1");
    when(activities.bookHotel(hotelReqIdCaptor.capture(), eq("Alice"))).thenReturn("hotel-1");
    when(activities.bookFlight(flightReqIdCaptor.capture(), eq("Alice"))).thenReturn("flight-1");
    when(activities.issueVisa(visaReqIdCaptor.capture(), eq("Alice"))).thenReturn("visa-1");

    doThrow(ApplicationFailure.newNonRetryableFailure("payment declined", "paymentFailure"))
        .when(activities)
        .charge(anyString(), anyInt());

    worker.registerActivitiesImplementations(activities);
    testEnv.start();

    WorkflowException ex = assertThrows(WorkflowException.class, () -> workflow.bookTrip("Alice"));

    // Walk the cause chain to the original ApplicationFailure the mock threw.
    Throwable cause = ex;
    while (cause != null && !(cause instanceof ApplicationFailure)) {
      cause = cause.getCause();
    }
    assertNotNull(cause, "Expected an ApplicationFailure somewhere in the cause chain");
    assertEquals("payment declined", ((ApplicationFailure) cause).getOriginalMessage());

    // Both leg compensation child workflows ran and cancelled every reservation.
    verify(activities).cancelCar(carReqIdCaptor.getValue());
    verify(activities).cancelHotel(hotelReqIdCaptor.getValue());
    verify(activities).cancelFlight(flightReqIdCaptor.getValue());
    verify(activities).cancelVisa(visaReqIdCaptor.getValue());
  }

  /**
   * If a leg fails internally, its own Saga rolls back the leg's reservations before the failure
   * reaches the parent. The parent then has nothing registered yet for the failed leg and sees no
   * sibling-leg to undo (it never started the second leg).
   */
  @Test
  public void legSelfRollsBackWhenInternalActivityFails(
      TestWorkflowEnvironment testEnv,
      Worker worker,
      NestedSaga.TripBookingParentWorkflow workflow) {
    NestedSaga.BookingActivities activities = mock(NestedSaga.BookingActivities.class);

    ArgumentCaptor<String> carReqIdCaptor = ArgumentCaptor.forClass(String.class);

    when(activities.reserveCar(carReqIdCaptor.capture(), eq("Bob"))).thenReturn("car-1");
    doThrow(ApplicationFailure.newNonRetryableFailure("hotel full", "hotelFailure"))
        .when(activities)
        .bookHotel(anyString(), eq("Bob"));

    worker.registerActivitiesImplementations(activities);
    testEnv.start();

    assertThrows(WorkflowException.class, () -> workflow.bookTrip("Bob"));

    // Domestic leg's own Saga cancelled the car reservation it had made.
    verify(activities).cancelCar(carReqIdCaptor.getValue());
  }
}
