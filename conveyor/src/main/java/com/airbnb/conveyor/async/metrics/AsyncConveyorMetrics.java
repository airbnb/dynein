/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async.metrics;

import io.github.resilience4j.bulkhead.Bulkhead;
import lombok.NonNull;

public interface AsyncConveyorMetrics {
  default void add(final long elapsedTime, String queueName) {}

  default void addFailure(@NonNull final Throwable error, String queueName) {}

  default void consume(final long elapsedTime, String queueName) {}

  default void consumePermitted() {}

  default void consumeRejected() {}

  default void consumeFinished() {}

  default void consumeFailure(@NonNull final Throwable error, String queueName) {}

  default void bulkheadMetrics(@NonNull final Bulkhead.Metrics metrics) {}
}
