/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler;

import com.airbnb.conveyor.async.AsyncSqsClient;
import com.airbnb.dynein.api.DyneinJobSpec;
import com.airbnb.dynein.api.JobSchedulePolicy;
import com.airbnb.dynein.api.PrepareJobRequest;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.common.utils.TimeUtils;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class Scheduler {
  @NonNull
  @Named(Constants.SQS_PRODUCER)
  private final AsyncSqsClient asyncClient;

  @NonNull
  @Named(Constants.INBOUND_QUEUE_NAME)
  private final String inboundQueueName;

  @NonNull private final JobSpecTransformer jobSpecTransformer;
  @NonNull private final TokenManager tokenManager;
  @NonNull private final ScheduleManager scheduleManager;
  @NonNull private final Clock clock;
  @NonNull private final Metrics metrics;

  public String prepareJob(@NonNull PrepareJobRequest request) {
    return tokenManager.generateToken(
        request.getAssociatedId(),
        null,
        request
            .getSchedulePolicyOptional()
            .flatMap(JobSchedulePolicy::getEpochMillisOptional)
            .orElse(null));
  }

  public CompletableFuture<Void> createJob(@NonNull DyneinJobSpec jobSpec) {
    CompletableFuture<Void> ret = new CompletableFuture<>();
    log.info("Create new job {}", jobSpec);
    String serializedJobSpec = jobSpecTransformer.serializeJobSpec(jobSpec);

    switch (jobSpec.getSchedulePolicy().getType()) {
      case IMMEDIATE:
        {
          ret =
              asyncClient
                  .add(serializedJobSpec, jobSpec.getQueueName())
                  .whenComplete(
                      (it, ex) -> {
                        if (ex == null) {
                          log.info(
                              "DyneinJob {} triggered, enqueue to queue {}, type: IMMEDIATE",
                              jobSpec.getJobToken(),
                              jobSpec.getQueueName());
                          metrics.dispatchJob(jobSpec.getQueueName());
                        } else {
                          log.error(
                              "Failed to trigger DyneinJob {} to queue {}, type: IMMEDIATE",
                              jobSpec.getJobToken(),
                              jobSpec.getQueueName(),
                              ex);
                        }
                      });
          break;
        }
      case SCHEDULED:
        {
          ret =
              asyncClient
                  .add(serializedJobSpec, inboundQueueName)
                  .whenComplete(
                      (it, ex) -> {
                        if (ex == null) {
                          log.info(
                              "Added job spec {} to inbound job queue {}, with token {}",
                              jobSpec.getName(),
                              inboundQueueName,
                              jobSpec.getJobToken());
                          metrics.enqueueToInboundQueue(jobSpec);
                        } else {
                          log.error(
                              "Exception when adding job spec {} to inbound job queue {}, with token {}",
                              jobSpec.getName(),
                              inboundQueueName,
                              jobSpec.getJobToken(),
                              ex);
                        }
                      });
          break;
        }
      case SQS_DELAYED:
        {
          long delayMs = TimeUtils.getDelayMillis(jobSpec.getSchedulePolicy(), clock);

          if (delayMs > TimeUnit.MINUTES.toMillis(15)) {
            ret.completeExceptionally(
                new IllegalArgumentException(
                    "Delay "
                        + delayMs
                        + "ms is longer than 15 minutes, cannot be scheduled with SQS_DELAYED"));
          } else {
            ret =
                asyncClient
                    .add(
                        serializedJobSpec,
                        jobSpec.getQueueName(),
                        (int) TimeUnit.MILLISECONDS.toSeconds(delayMs))
                    .whenComplete(
                        (it, ex) -> {
                          if (ex == null) {
                            log.info(
                                "DyneinJob {} triggered, enqueue to queue {}, type: SQS_DELAYED",
                                jobSpec.getJobToken(),
                                jobSpec.getQueueName());
                            metrics.dispatchJob(jobSpec.getQueueName());
                          } else {
                            log.error(
                                "Failed to trigger DyneinJob {} to queue {}, type: SQS_DELAYED",
                                jobSpec.getJobToken(),
                                jobSpec.getQueueName(),
                                ex);
                          }
                        });
          }
          break;
        }
      default:
        ret.completeExceptionally(
            new UnsupportedOperationException("Unsupported Job Schedule Policy Type"));
    }
    return ret;
  }

  public CompletableFuture<Void> deleteJob(String token) {
    return scheduleManager.deleteJob(token);
  }

  public CompletableFuture<DyneinJobSpec> getJob(String token) {
    return scheduleManager
        .getJob(token)
        .thenApply(schedule -> jobSpecTransformer.deserializeJobSpec(schedule.getJobSpec()));
  }
}
