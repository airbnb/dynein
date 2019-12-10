/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async.mock;

import com.airbnb.conveyor.async.AsyncConsumer;
import com.airbnb.conveyor.async.AsyncSqsClient;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** Represents an in-memory blocking {@code AsyncClient} implement. */
@Slf4j
public final class MockAsyncSqsClient implements AsyncSqsClient {
  private final int DEFAULT_CAPACITY = 10_000;

  @NonNull private final BlockingQueue<String> queue;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final ThreadPoolExecutor executor =
      new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

  public MockAsyncSqsClient() {
    this.queue = new ArrayBlockingQueue<>(DEFAULT_CAPACITY, true);
  }

  private CompletableFuture<String> getMessage() {
    CompletableFuture<String> message = new CompletableFuture<>();
    CompletableFuture.runAsync(
        () -> {
          try {
            final String body = queue.take();
            message.complete(body);
          } catch (Exception ex) {
            message.completeExceptionally(ex);
          }
        },
        executor);
    return message;
  }

  private CompletableFuture<Boolean> consumeInternal(
      BiFunction<String, Executor, CompletableFuture<Void>> compute) {
    CompletableFuture<Boolean> ret = new CompletableFuture<>();
    if (queue.size() == 0) {
      ret.complete(false);
    } else {
      getMessage()
          .whenComplete(
              (res, exception) -> {
                if (exception != null) {
                  ret.completeExceptionally(exception);
                }
              })
          .thenCompose(
              message -> {
                CompletableFuture<Void> result = compute.apply(message, executor);
                return result.whenComplete(
                    (it, ex) -> {
                      if (ex == null) {
                        ret.complete(true);
                      } else {
                        ret.completeExceptionally(ex);
                      }
                    });
              });
    }
    return ret;
  }

  @Override
  public CompletableFuture<Void> add(@NonNull String message, @NonNull String queueName) {
    queue.add(message);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> add(
      @NonNull String message, @NonNull String queueName, int delaySeconds) {
    if (delaySeconds < 0 || delaySeconds > 900) {
      throw new IllegalArgumentException("Valid values for DelaySeconds is 0 to 900.");
    }
    CompletableFuture<Void> ret = new CompletableFuture<>();
    scheduler.schedule(
        () -> {
          add(message, queueName);
          ret.complete(null);
        },
        delaySeconds,
        TimeUnit.SECONDS);
    return ret;
  }

  @Override
  public CompletableFuture<Boolean> consume(
      @NonNull final Consumer<String> consumer, @NonNull final String queueName) {
    return consumeInternal(
        (body, executor) -> CompletableFuture.runAsync(() -> consumer.accept(body), executor));
  }

  @Override
  public CompletableFuture<Boolean> consume(AsyncConsumer<String> consumer, String queueName) {
    return consumeInternal(consumer::accept);
  }

  @Override
  public void close() {}
}
