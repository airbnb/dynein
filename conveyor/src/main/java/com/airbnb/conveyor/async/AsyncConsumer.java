/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface AsyncConsumer<T> {
  CompletableFuture<Void> accept(T t, Executor executor);
}
