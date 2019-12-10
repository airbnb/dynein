/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.NonNull;

public interface AsyncSqsClient extends Closeable {
  CompletableFuture<Void> add(@NonNull final String message, @NonNull final String queueName);

  CompletableFuture<Void> add(
      @NonNull final String message, @NonNull final String queueName, int delaySeconds);

  CompletableFuture<Boolean> consume(
      @NonNull final Consumer<String> consumer, @NonNull final String queueName);

  CompletableFuture<Boolean> consume(AsyncConsumer<String> consumer, String queueName);
}
