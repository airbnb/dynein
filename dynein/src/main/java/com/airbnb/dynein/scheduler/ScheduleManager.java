/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler;

import com.airbnb.dynein.api.DyneinJobSpec;
import com.airbnb.dynein.api.InvalidTokenException;
import com.airbnb.dynein.api.JobTokenPayload;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.common.utils.TimeUtils;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import java.io.Closeable;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class ScheduleManager implements Closeable {

  @Value
  @AllArgsConstructor(staticName = "of")
  public static class SchedulesQueryResponse {
    List<Schedule> schedules;
    boolean shouldImmediatelyQueryAgain;
  }

  protected final int maxShardId;
  protected final TokenManager tokenManager;
  protected final JobSpecTransformer jobSpecTransformer;
  protected final Clock clock;
  protected final Metrics metrics;

  public abstract CompletableFuture<Void> addJob(Schedule schedule);

  public abstract CompletableFuture<Schedule> getJob(String token);

  public abstract CompletableFuture<Void> deleteJob(String token);

  public abstract CompletableFuture<SchedulesQueryResponse> getOverdueJobs(
      String partition, Instant instant);

  public abstract CompletableFuture<Void> recoverStuckJobs(String partition, Instant instant);

  public final CompletableFuture<SchedulesQueryResponse> getOverdueJobs(String partition) {
    return getOverdueJobs(partition, Instant.now(clock));
  }

  public abstract CompletableFuture<Schedule> updateStatus(
      Schedule schedule, Schedule.JobStatus oldStatus, Schedule.JobStatus newStatus);

  public abstract CompletableFuture<Void> deleteDispatchedJob(Schedule schedule);

  protected Schedule makeSchedule(String serializedJobSpec) throws InvalidTokenException {
    DyneinJobSpec jobSpec = jobSpecTransformer.deserializeJobSpec(serializedJobSpec);
    String date =
        Long.toString(TimeUtils.getInstant(jobSpec.getSchedulePolicy(), clock).toEpochMilli());
    JobTokenPayload token = tokenManager.decodeToken(jobSpec.getJobToken());
    int shard = token.getLogicalShard();
    return new Schedule(
        String.format("%s#%s", date, jobSpec.getJobToken()),
        Schedule.JobStatus.SCHEDULED,
        serializedJobSpec,
        Integer.toString(Math.floorMod(shard, maxShardId)));
  }
}
