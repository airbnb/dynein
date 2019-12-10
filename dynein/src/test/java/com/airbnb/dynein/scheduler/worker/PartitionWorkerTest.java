/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.worker;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.airbnb.conveyor.async.AsyncSqsClient;
import com.airbnb.dynein.api.*;
import com.airbnb.dynein.common.job.JacksonJobSpecTransformer;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.JacksonTokenManager;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.scheduler.NoOpScheduleManager;
import com.airbnb.dynein.scheduler.Schedule;
import com.airbnb.dynein.scheduler.Schedule.JobStatus;
import com.airbnb.dynein.scheduler.ScheduleManager;
import com.airbnb.dynein.scheduler.ScheduleManager.SchedulesQueryResponse;
import com.airbnb.dynein.scheduler.config.WorkersConfiguration;
import com.airbnb.dynein.scheduler.metrics.NoOpMetricsImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PartitionWorkerTest {

  private static final byte[] SERIALIZED_JOB_DATA = {0, 0, 0, 0};
  @Mock private AsyncSqsClient asyncClient;
  private TokenManager tokenManager;
  private Clock clock;
  private JobSpecTransformer transformer;
  private PartitionWorker worker;
  private String validToken;
  @Mock private ScheduleManager scheduleManager;

  @Before
  public void setUp() {
    clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"));
    tokenManager = new JacksonTokenManager(new ObjectMapper());
    validToken = tokenManager.generateToken(2, "test-cluster", clock.millis() + 1000);
    ObjectMapper mapper = new ObjectMapper();
    transformer = new JacksonJobSpecTransformer(mapper);
    tokenManager = new JacksonTokenManager(mapper);
    scheduleManager =
        spy(new NoOpScheduleManager(64, tokenManager, transformer, clock, new NoOpMetricsImpl()));
    worker =
        new PartitionWorker(
            1,
            asyncClient,
            new EventBus(),
            clock,
            scheduleManager,
            transformer,
            new WorkersConfiguration(1, 10, 1000),
            new NoOpMetricsImpl());
    worker.startExecutor();
    when(scheduleManager.updateStatus(
            any(Schedule.class), eq(JobStatus.SCHEDULED), eq(JobStatus.ACQUIRED)))
        .thenAnswer(
            invocation ->
                CompletableFuture.completedFuture(
                    ((Schedule) invocation.getArguments()[0]).withStatus(JobStatus.ACQUIRED)));
  }

  // lifted from original DyneinTest
  private DyneinJobSpec getTestJobSpec(String token, String queueName) {
    JobSchedulePolicy policy;
    policy =
        JobSchedulePolicy.builder()
            .type(JobScheduleType.SCHEDULED)
            .epochMillis(Instant.now().plusMillis(1000).toEpochMilli())
            .build();

    return DyneinJobSpec.builder()
        .jobToken(token)
        .name("AddJob")
        .queueType("PRODUCTION")
        .queueName(queueName)
        .createAtInMillis(Instant.now().minusMillis(10).toEpochMilli())
        .schedulePolicy(policy)
        .serializedJob(SERIALIZED_JOB_DATA)
        .build();
  }

  private Schedule getSchedule(DyneinJobSpec jobSpec, boolean failure) throws Exception {
    JobTokenPayload payload = tokenManager.decodeToken(jobSpec.getJobToken());
    int shard = !failure ? payload.getLogicalShard() : 1;
    String message = transformer.serializeJobSpec(jobSpec);

    return new Schedule(
        payload
                .getEpochMillisOptional()
                .orElseThrow(
                    () -> new IllegalStateException("no tokens without epochMillis allowed"))
            + "-"
            + jobSpec.getJobToken(),
        Schedule.JobStatus.SCHEDULED,
        message,
        Integer.toString(shard));
  }

  @Test
  public void testDispatchJob() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test3");
    Schedule schedule = getSchedule(jobSpec, false);

    when(asyncClient.add(schedule.getJobSpec(), "test3"))
        .thenReturn(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> ret = worker.dispatchToDestination(schedule);

    ret.get(1000, TimeUnit.MILLISECONDS);

    verify(asyncClient, times(1)).add(schedule.getJobSpec(), "test3");
    verify(scheduleManager, times(1)).deleteDispatchedJob(eq(schedule));
    verifyNoMoreInteractions(asyncClient, scheduleManager);
  }

  @Test
  public void testDispatch_EnqueueFail() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test4");
    Schedule schedule = getSchedule(jobSpec, false);

    Exception addException = new Exception();
    CompletableFuture<Void> error = new CompletableFuture<>();
    error.completeExceptionally(addException);

    when(asyncClient.add(schedule.getJobSpec(), "test4")).thenReturn(error);

    CompletableFuture<Void> ret = worker.dispatchToDestination(schedule);

    try {
      ret.get(1000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      assertSame(e.getCause(), addException);
    }
    verify(asyncClient, times(1)).add(schedule.getJobSpec(), "test4");
    verify(scheduleManager, times(1))
        .updateStatus(schedule, Schedule.JobStatus.ACQUIRED, Schedule.JobStatus.SCHEDULED);
    verifyNoMoreInteractions(scheduleManager, asyncClient);
  }

  public void testDispatch_ResetToScheduledFail() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test5");
    Schedule schedule = getSchedule(jobSpec, false);

    CompletableFuture<Schedule> updateError = new CompletableFuture<>();
    updateError.completeExceptionally(new Exception());
    CompletableFuture<Void> queueError = new CompletableFuture<>();
    queueError.completeExceptionally(new Exception());
    when(asyncClient.add(schedule.getJobSpec(), "test5")).thenReturn(queueError);
    when(scheduleManager.updateStatus(
            schedule, Schedule.JobStatus.ACQUIRED, Schedule.JobStatus.SCHEDULED))
        .thenReturn(updateError);

    CompletableFuture<Void> ret = worker.dispatchToDestination(schedule);

    Throwable caught = null;
    try {
      ret.get(1000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      caught = e.getCause();
    }
    assertSame(caught, updateError);

    verify(asyncClient, times(1)).add(schedule.getJobSpec(), "test5");
    verify(scheduleManager, times(1))
        .updateStatus(schedule, Schedule.JobStatus.ACQUIRED, Schedule.JobStatus.SCHEDULED);
    verifyNoMoreInteractions(scheduleManager, asyncClient);
  }

  @Test
  public void testDispatch_DeleteFromTableFail() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test6");
    Schedule schedule = getSchedule(jobSpec, false);
    CompletableFuture<Void> response = new CompletableFuture<>();
    Exception exception = new Exception();
    response.completeExceptionally(exception);
    when(asyncClient.add(schedule.getJobSpec(), "test6"))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(scheduleManager.deleteDispatchedJob(schedule)).thenReturn(response);

    CompletableFuture<Void> ret = worker.dispatchToDestination(schedule);

    ret.get(1000, TimeUnit.MILLISECONDS);

    verify(asyncClient, times(1)).add(schedule.getJobSpec(), "test6");
    verify(scheduleManager, times(1)).deleteDispatchedJob(schedule);
    verifyNoMoreInteractions(scheduleManager, asyncClient);
  }

  @Test
  public void testDispatchOverdue() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test7");
    Schedule schedule = getSchedule(jobSpec, false);
    Schedule scheduleAcquired = schedule.withStatus(JobStatus.ACQUIRED);
    List<Schedule> items = asList(schedule, schedule);
    String partition = "1";
    SchedulesQueryResponse response = SchedulesQueryResponse.of(items, false);

    when(scheduleManager.getOverdueJobs(partition))
        .thenReturn(CompletableFuture.completedFuture(response));
    when(asyncClient.add(schedule.getJobSpec(), "test7"))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(scheduleManager.deleteDispatchedJob(scheduleAcquired))
        .thenReturn(CompletableFuture.completedFuture(null));

    CompletableFuture<Boolean> ret = worker.dispatchOverdue(partition);

    Assert.assertEquals(
        ret.get(1000, TimeUnit.MILLISECONDS), response.isShouldImmediatelyQueryAgain());

    verify(scheduleManager, times(1)).getOverdueJobs(partition);
    verify(scheduleManager, times(2))
        .updateStatus(schedule, Schedule.JobStatus.SCHEDULED, Schedule.JobStatus.ACQUIRED);
    verify(asyncClient, times(2)).add(schedule.getJobSpec(), "test7");
    verify(scheduleManager, times(2)).deleteDispatchedJob(scheduleAcquired);
    verifyNoMoreInteractions(scheduleManager, asyncClient);
  }

  @Test
  public void testDispatchOverdue_scanForOverdueFailure() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test8");
    Schedule schedule = getSchedule(jobSpec, false);
    String partition = "failure1";
    CompletableFuture<SchedulesQueryResponse> response = new CompletableFuture<>();
    response.completeExceptionally(new Exception());

    when(scheduleManager.getOverdueJobs(partition)).thenReturn(response);
    CompletableFuture<Boolean> ret = worker.dispatchOverdue(partition);

    ret.get(1000, TimeUnit.MILLISECONDS);

    verify(scheduleManager, times(1)).getOverdueJobs(partition);
    verifyNoMoreInteractions(scheduleManager, asyncClient);
  }

  @Test
  public void testDispatchOverdue_SetAcquiredPartialFailure() throws Exception {
    DyneinJobSpec jobSpec_1 = getTestJobSpec(validToken, "test9");
    Schedule schedule_1 = getSchedule(jobSpec_1, false);
    DyneinJobSpec jobSpec_2 = getTestJobSpec(validToken, "test9");
    Schedule schedule_2 = getSchedule(jobSpec_2, true);

    String partition = "failure2";
    CompletableFuture<Schedule> updateResponse = new CompletableFuture<>();
    updateResponse.completeExceptionally(new Exception());

    when(scheduleManager.getOverdueJobs(partition))
        .thenReturn(
            CompletableFuture.completedFuture(
                SchedulesQueryResponse.of(asList(schedule_1, schedule_2), false)));
    when(scheduleManager.updateStatus(schedule_2, JobStatus.SCHEDULED, JobStatus.ACQUIRED))
        .thenReturn(updateResponse);
    when(asyncClient.add(schedule_1.getJobSpec(), "test9"))
        .thenReturn(CompletableFuture.completedFuture(null));

    CompletableFuture<Boolean> ret = worker.dispatchOverdue("failure2");

    ret.get(1000, TimeUnit.MILLISECONDS);

    verify(scheduleManager, times(1)).getOverdueJobs(partition);
    verify(scheduleManager, times(1))
        .updateStatus(schedule_1, JobStatus.SCHEDULED, JobStatus.ACQUIRED);
    verify(scheduleManager, times(1))
        .updateStatus(schedule_2, JobStatus.SCHEDULED, JobStatus.ACQUIRED);
    verify(asyncClient, times(1)).add(schedule_1.getJobSpec(), "test9");
    verify(scheduleManager, times(1))
        .deleteDispatchedJob(schedule_1.withStatus(JobStatus.ACQUIRED));
    verifyNoMoreInteractions(scheduleManager, asyncClient);
  }

  // test dispatched overdue w/ dispatch fail
  @Test
  public void testDispatchOverdue_dispatchPartialFailure() throws Exception {
    String partition = "failure3";
    DyneinJobSpec jobSpec_1 = getTestJobSpec(validToken, "test10");
    Schedule schedule_1 = getSchedule(jobSpec_1, false);
    DyneinJobSpec jobSpec_2 = getTestJobSpec(validToken, "test10.1");
    Schedule schedule_2 = getSchedule(jobSpec_2, true);

    CompletableFuture<Void> dispatchResponse = new CompletableFuture<>();
    dispatchResponse.completeExceptionally(new Exception());

    when(scheduleManager.getOverdueJobs(partition))
        .thenReturn(
            CompletableFuture.completedFuture(
                SchedulesQueryResponse.of(asList(schedule_1, schedule_2), false)));

    when(asyncClient.add(schedule_1.getJobSpec(), "test10"))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(asyncClient.add(schedule_2.getJobSpec(), "test10.1")).thenReturn(dispatchResponse);
    when(scheduleManager.updateStatus(
            schedule_2.withStatus(JobStatus.ACQUIRED),
            Schedule.JobStatus.ACQUIRED,
            Schedule.JobStatus.SCHEDULED))
        .thenReturn(CompletableFuture.completedFuture(schedule_2));

    CompletableFuture<Boolean> ret = worker.dispatchOverdue(partition);

    ret.get(1000, TimeUnit.MILLISECONDS);

    verify(scheduleManager, times(1)).getOverdueJobs(partition);
    verify(scheduleManager, times(1))
        .updateStatus(schedule_1, JobStatus.SCHEDULED, JobStatus.ACQUIRED);
    verify(scheduleManager, times(1))
        .updateStatus(schedule_2, JobStatus.SCHEDULED, JobStatus.ACQUIRED);
    verify(asyncClient, times(1)).add(schedule_1.getJobSpec(), "test10");
    verify(asyncClient, times(1)).add(schedule_2.getJobSpec(), "test10.1");
    verify(scheduleManager, times(1))
        .deleteDispatchedJob(schedule_1.withStatus(JobStatus.ACQUIRED));
    verify(scheduleManager, times(1))
        .updateStatus(
            schedule_2.withStatus(JobStatus.ACQUIRED), JobStatus.ACQUIRED, JobStatus.SCHEDULED);
    verifyNoMoreInteractions(scheduleManager, asyncClient);
  }
}
