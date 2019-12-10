/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.common.utils;

import com.airbnb.dynein.api.JobSchedulePolicy;
import com.airbnb.dynein.api.JobScheduleType;
import com.google.common.base.Preconditions;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TimeUtils {

  private static IllegalStateException memberMissingException =
      new IllegalStateException("At least one of `epochMillis` or `delayMillis` must be present.");

  public long getDelayMillis(JobSchedulePolicy policy, Clock clock) {
    return policy
        .getDelayMillisOptional()
        .orElseGet(
            () ->
                policy
                    .getEpochMillisOptional()
                    .map(epoch -> epoch - Instant.now(clock).toEpochMilli())
                    .orElseThrow(() -> memberMissingException));
  }

  public Instant getInstant(JobSchedulePolicy policy, Clock clock) {
    return policy
        .getEpochMillisOptional()
        .map(Instant::ofEpochMilli)
        .orElseGet(
            () ->
                policy
                    .getDelayMillisOptional()
                    .map(delay -> Instant.now(clock).plusMillis(delay))
                    .orElseThrow(() -> memberMissingException));
  }

  public JobSchedulePolicy getPreferredSchedulePolicy(Duration delay, Clock clock) {
    Preconditions.checkArgument(
        !delay.isNegative(), "Scheduled Delay should be positive, got %s", delay);
    if (delay.equals(Duration.ZERO)) {
      return getSchedulePolicyWithEpochMillis(delay, JobScheduleType.IMMEDIATE, clock);
    } else if (delay.minus(Duration.ofMinutes(15)).isNegative()) {
      return getSchedulePolicyWithEpochMillis(delay, JobScheduleType.SQS_DELAYED, clock);
    } else {
      return getSchedulePolicyWithEpochMillis(delay, JobScheduleType.SCHEDULED, clock);
    }
  }

  public JobSchedulePolicy getSchedulePolicyWithEpochMillis(
      Duration delay, JobScheduleType scheduleType, Clock clock) {
    return JobSchedulePolicy.builder()
        .epochMillis(Instant.now(clock).plus(delay).toEpochMilli())
        .type(scheduleType)
        .build();
  }
}
