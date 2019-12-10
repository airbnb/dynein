/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.airbnb.conveyor.async.AsyncSqsClient;
import com.airbnb.dynein.api.DyneinJobSpec;
import com.airbnb.dynein.api.JobSchedulePolicy;
import com.airbnb.dynein.api.JobScheduleType;
import com.airbnb.dynein.common.job.JacksonJobSpecTransformer;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.JacksonTokenManager;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.scheduler.metrics.NoOpMetricsImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchedulerTest {
  @Mock private AsyncSqsClient asyncClient;
  @Mock private ScheduleManager scheduleManager;
  private Scheduler scheduler;
  private JobSpecTransformer jobSpecTransformer;

  @Before
  public void setUp() {
    ObjectMapper mapper = new ObjectMapper();
    jobSpecTransformer = new JacksonJobSpecTransformer(mapper);
    TokenManager tokenManager = new JacksonTokenManager(mapper);
    scheduler =
        new Scheduler(
            asyncClient,
            "inbound-test",
            jobSpecTransformer,
            tokenManager,
            scheduleManager,
            Clock.fixed(Instant.now(), ZoneId.of("UTC")),
            new NoOpMetricsImpl());
  }

  @Test
  public void testScheduledJob() {
    DyneinJobSpec jobSpec =
        DyneinJobSpec.builder()
            .name("AddJob")
            .queueName("test-queue")
            .schedulePolicy(
                JobSchedulePolicy.builder()
                    .type(JobScheduleType.SCHEDULED)
                    .delayMillis(1000L)
                    .build())
            .build();
    when(asyncClient.add(jobSpecTransformer.serializeJobSpec(jobSpec), "inbound-test"))
        .thenReturn(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> ret = scheduler.createJob(jobSpec);
    verify(asyncClient).add(jobSpecTransformer.serializeJobSpec(jobSpec), "inbound-test");
    assertNull(ret.join());
  }

  @Test
  public void testImmediateJob() {
    when(asyncClient.add(any(String.class), eq("test-queue")))
        .thenReturn(CompletableFuture.completedFuture(null));

    DyneinJobSpec jobSpec =
        DyneinJobSpec.builder()
            .name("AddJob")
            .queueName("test-queue")
            .schedulePolicy(
                JobSchedulePolicy.builder().type(JobScheduleType.IMMEDIATE).delayMillis(0L).build())
            .build();
    CompletableFuture<Void> ret = scheduler.createJob(jobSpec);
    verify(asyncClient).add(jobSpecTransformer.serializeJobSpec(jobSpec), "test-queue");
    assertNull(ret.join());
  }

  @Test
  public void testSQSDelayedJob_withInstant() {
    when(asyncClient.add(any(String.class), eq("test-queue"), any(Integer.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    DyneinJobSpec jobSpec =
        DyneinJobSpec.builder()
            .name("AddJob")
            .queueName("test-queue")
            .schedulePolicy(
                JobSchedulePolicy.builder()
                    .type(JobScheduleType.SQS_DELAYED)
                    .delayMillis(5000L)
                    .build())
            .build();
    CompletableFuture<Void> ret = scheduler.createJob(jobSpec);
    verify(asyncClient).add(jobSpecTransformer.serializeJobSpec(jobSpec), "test-queue", 5);
    assertNull(ret.join());
  }

  @Test
  public void testSQSDelayedJob_withDelay() {
    when(asyncClient.add(any(String.class), eq("test-queue"), any(Integer.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    DyneinJobSpec jobSpec =
        DyneinJobSpec.builder()
            .name("AddJob")
            .queueName("test-queue")
            .schedulePolicy(
                JobSchedulePolicy.builder()
                    .type(JobScheduleType.SQS_DELAYED)
                    .epochMillis(Instant.now().plusMillis(5000L).toEpochMilli())
                    .build())
            .build();
    CompletableFuture<Void> ret = scheduler.createJob(jobSpec);
    verify(asyncClient).add(jobSpecTransformer.serializeJobSpec(jobSpec), "test-queue", 5);
    assertNull(ret.join());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSQSDelayedJob_DelayTooLong() throws Throwable {
    DyneinJobSpec jobSpec =
        DyneinJobSpec.builder()
            .name("AddJob")
            .queueName("error-queue")
            .schedulePolicy(
                JobSchedulePolicy.builder()
                    .type(JobScheduleType.SQS_DELAYED)
                    .epochMillis(Instant.now().plusMillis(1000000L).toEpochMilli())
                    .build())
            .build();
    try {
      scheduler.createJob(jobSpec).join();
    } catch (Exception e) {
      Throwable t = e;
      while (t.getCause() != null) {
        t = t.getCause();
      }
      throw t;
    }
  }

  @Test
  public void testScheduledJobFailure() {
    DyneinJobSpec jobSpec =
        DyneinJobSpec.builder()
            .name("AddJob")
            .queueName("error-queue")
            .schedulePolicy(
                JobSchedulePolicy.builder()
                    .type(JobScheduleType.SCHEDULED)
                    .delayMillis(1000L)
                    .build())
            .build();
    CompletableFuture<Void> error = new CompletableFuture<>();
    error.completeExceptionally(new Exception());
    when(asyncClient.add(jobSpecTransformer.serializeJobSpec(jobSpec), "inbound-test"))
        .thenReturn(error);

    CompletableFuture<Void> ret = scheduler.createJob(jobSpec);
    verify(asyncClient).add(jobSpecTransformer.serializeJobSpec(jobSpec), "inbound-test");

    try {
      ret.get(1000, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeout) {
      fail("Future does not seem to complete when failure in adding to inbound queue (SCHEDULED).");
    } catch (Exception ex) {
    }
  }

  @Test
  public void testImmediateJobFailure() {
    DyneinJobSpec jobSpec =
        DyneinJobSpec.builder()
            .name("AddJob")
            .queueName("test-error")
            .schedulePolicy(
                JobSchedulePolicy.builder().type(JobScheduleType.IMMEDIATE).delayMillis(0L).build())
            .build();
    CompletableFuture<Void> error = new CompletableFuture<>();
    error.completeExceptionally(new Exception());
    when(asyncClient.add(jobSpecTransformer.serializeJobSpec(jobSpec), "test-error"))
        .thenReturn(error);

    CompletableFuture<Void> ret = scheduler.createJob(jobSpec);
    verify(asyncClient).add(jobSpecTransformer.serializeJobSpec(jobSpec), "test-error");

    try {
      ret.get(1000, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeout) {
      fail(
          "Future does not seem to complete when failure in adding to destination queue (IMMEDIATE).");
    } catch (Exception ex) {
    }
  }

  @Test
  public void testSQSDelayedJobFailure() {
    DyneinJobSpec jobSpec =
        DyneinJobSpec.builder()
            .name("AddJob")
            .queueName("test-error")
            .schedulePolicy(
                JobSchedulePolicy.builder()
                    .type(JobScheduleType.SQS_DELAYED)
                    .epochMillis(Instant.now().plusMillis(5000L).toEpochMilli())
                    .build())
            .build();

    CompletableFuture<Void> error = new CompletableFuture<>();
    error.completeExceptionally(new Exception());
    when(asyncClient.add(jobSpecTransformer.serializeJobSpec(jobSpec), "test-error", 5))
        .thenReturn(error);

    CompletableFuture<Void> ret = scheduler.createJob(jobSpec);
    verify(asyncClient).add(jobSpecTransformer.serializeJobSpec(jobSpec), "test-error", 5);

    try {
      ret.get(1000, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeout) {
      fail(
          "Future does not seem to complete when failure in adding to destination queue (SQS_DELAYED).");
    } catch (Exception ex) {
    }
  }
}
