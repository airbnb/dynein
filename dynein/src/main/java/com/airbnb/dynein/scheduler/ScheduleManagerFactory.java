/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler;

import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import java.time.Clock;
import javax.inject.Provider;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class ScheduleManagerFactory implements Provider<ScheduleManager> {
  protected final int maxShardId;
  protected final TokenManager tokenManager;
  protected final JobSpecTransformer jobSpecTransformer;
  protected final Clock clock;
  protected final Metrics metrics;

  @Override
  public abstract ScheduleManager get();
}
