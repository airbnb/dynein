/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler;

import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.scheduler.Schedule.JobStatus;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class NoOpScheduleManager extends ScheduleManager {

  public NoOpScheduleManager(
      int maxShardId,
      TokenManager tokenManager,
      JobSpecTransformer jobSpecTransformer,
      Clock clock,
      Metrics metrics) {
    super(maxShardId, tokenManager, jobSpecTransformer, clock, metrics);
  }

  @Override
  public CompletableFuture<Void> recoverStuckJobs(String partition, Instant instant) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> addJob(Schedule schedule) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Schedule> getJob(String token) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> deleteJob(String token) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<SchedulesQueryResponse> getOverdueJobs(
      String partition, Instant instant) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Schedule> updateStatus(
      Schedule schedule, JobStatus oldStatus, JobStatus newStatus) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> deleteDispatchedJob(Schedule schedule) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {}
}
