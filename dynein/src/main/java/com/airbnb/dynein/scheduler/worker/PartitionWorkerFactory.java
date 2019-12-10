/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.worker;

import com.airbnb.conveyor.async.AsyncSqsClient;
import com.airbnb.conveyor.async.AsyncSqsClientFactory;
import com.airbnb.conveyor.async.config.AsyncSqsClientConfiguration;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.scheduler.Constants;
import com.airbnb.dynein.scheduler.ScheduleManagerFactory;
import com.airbnb.dynein.scheduler.config.WorkersConfiguration;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import com.google.common.eventbus.EventBus;
import java.time.Clock;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public class PartitionWorkerFactory {
  private final AsyncSqsClientFactory asyncSqsClientFactory;

  @Named(Constants.SQS_PRODUCER)
  private final AsyncSqsClientConfiguration asyncSqsClientConfiguration;

  private final ScheduleManagerFactory scheduleManagerFactory;
  private final Clock clock;
  private final JobSpecTransformer jobSpecTransformer;
  private final WorkersConfiguration workersConfiguration;
  private final EventBus eventBus;
  private final Metrics metrics;

  private AsyncSqsClient getAsyncClient() {
    return asyncSqsClientFactory.create(asyncSqsClientConfiguration);
  }

  public PartitionWorker get(int index) {
    return new PartitionWorker(
        index,
        getAsyncClient(),
        eventBus,
        clock,
        scheduleManagerFactory.get(),
        jobSpecTransformer,
        workersConfiguration,
        metrics);
  }
}
