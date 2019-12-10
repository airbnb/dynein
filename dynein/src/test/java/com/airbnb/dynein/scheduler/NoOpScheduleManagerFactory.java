/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler;

import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import com.airbnb.dynein.scheduler.metrics.NoOpMetricsImpl;
import java.time.Clock;

public class NoOpScheduleManagerFactory extends ScheduleManagerFactory {

  public NoOpScheduleManagerFactory(
      int maxShardId,
      TokenManager tokenManager,
      JobSpecTransformer jobSpecTransformer,
      Clock clock,
      Metrics metrics) {
    super(maxShardId, tokenManager, jobSpecTransformer, clock, metrics);
  }

  @Override
  public ScheduleManager get() {
    return new NoOpScheduleManager(
        maxShardId, tokenManager, jobSpecTransformer, clock, new NoOpMetricsImpl());
  }
}
