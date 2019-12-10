/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async;

import com.airbnb.conveyor.async.config.AsyncSqsClientConfiguration;
import com.airbnb.conveyor.async.metrics.AsyncConveyorMetrics;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

/** Represents queue responsible for asynchronously propagating serialized messages. */
@Slf4j
@ThreadSafe
@Accessors
public class AsyncSqsClientImpl implements AsyncSqsClient {
  @Getter final SqsAsyncClient client;
  @Getter final AsyncConveyorMetrics metrics;

  private final AsyncLoadingCache<String, String> urlCache;
  private final ExecutorService executor;
  private final int receiveWaitTimeoutSeconds;
  private final Bulkhead bulkhead;

  @Inject
  AsyncSqsClientImpl(
      @NonNull final SqsAsyncClient client,
      @NonNull final AsyncConveyorMetrics metrics,
      @NonNull final ExecutorService executor) {

    this(
        client,
        metrics,
        executor,
        AsyncSqsClientConfiguration.DEFAULT_URL_CACHE_SIZE,
        AsyncSqsClientConfiguration.DEFAULT_RECEIVE_WAIT_SECONDS,
        AsyncSqsClientConfiguration.DEFAULT_BULKHEAD_MAX_WAIT_MS,
        AsyncSqsClientConfiguration.DEFAULT_CONSUMER_CONCURRENCY);
  }

  AsyncSqsClientImpl(
      @NonNull final SqsAsyncClient client,
      @NonNull final AsyncConveyorMetrics metrics,
      @NonNull final ExecutorService executor,
      long maxCacheSize,
      int receiveWaitTimeoutSeconds,
      int bulkheadMaxWaitMillis,
      int consumerConcurrency) {

    this.client = client;
    this.metrics = metrics;
    this.urlCache = initUrlCache(maxCacheSize);
    this.executor = executor;
    this.receiveWaitTimeoutSeconds = receiveWaitTimeoutSeconds;
    this.bulkhead =
        Bulkhead.of(
            "conveyor-async",
            BulkheadConfig.custom()
                .maxConcurrentCalls(consumerConcurrency)
                .maxWaitTimeDuration(Duration.ofMillis(bulkheadMaxWaitMillis))
                .build());
    this.bulkhead
        .getEventPublisher()
        .onCallPermitted(event -> metrics.consumePermitted())
        .onCallFinished(event -> metrics.consumeFinished())
        .onCallRejected(event -> metrics.consumeRejected())
        .onEvent(event -> metrics.bulkheadMetrics(this.bulkhead.getMetrics()));
  }

  private CompletableFuture<String> getQueueUrl(@NonNull final String queueName) {
    GetQueueUrlRequest urlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
    return client.getQueueUrl(urlRequest).thenApply(GetQueueUrlResponse::queueUrl);
  }

  private AsyncLoadingCache<String, String> initUrlCache(long maxCacheSize) {
    return Caffeine.newBuilder()
        .maximumSize(maxCacheSize)
        .buildAsync((key, executor) -> getQueueUrl(key));
  }

  private CompletableFuture<ReceiveMessageResponse> getMessage(@NonNull final String queueName) {
    return urlCache
        .get(queueName)
        .thenCompose(
            url -> {
              ReceiveMessageRequest messageRequest =
                  ReceiveMessageRequest.builder()
                      .queueUrl(url)
                      .maxNumberOfMessages(1)
                      .waitTimeSeconds(receiveWaitTimeoutSeconds)
                      .build();
              return client.receiveMessage(messageRequest);
            });
  }

  private CompletableFuture<DeleteMessageResponse> deleteMessage(
      @NonNull final String queueName, @NonNull final String messageReceipt) {
    return urlCache
        .get(queueName)
        .thenCompose(
            url -> {
              DeleteMessageRequest deleteMessageReq =
                  DeleteMessageRequest.builder()
                      .queueUrl(url)
                      .receiptHandle(messageReceipt)
                      .build();
              return client.deleteMessage(deleteMessageReq);
            });
  }

  private void setMessageVisibility(
      @NonNull final String queueName, @NonNull final String messageReceipt) {
    urlCache
        .get(queueName)
        .thenCompose(
            url -> {
              ChangeMessageVisibilityRequest messageVisibilityReq =
                  ChangeMessageVisibilityRequest.builder()
                      .queueUrl(url)
                      .receiptHandle(messageReceipt)
                      .visibilityTimeout(0)
                      .build();
              return client.changeMessageVisibility(messageVisibilityReq);
            });
  }

  private void consumePostProcess(
      @NonNull final String queueName,
      @NonNull final String messageReceipt,
      @NonNull CompletableFuture<Boolean> ret,
      @NonNull CompletableFuture<Void> computation,
      Stopwatch stopwatch) {
    computation.whenComplete(
        (r, ex) -> {
          if (ex == null) {
            deleteMessage(queueName, messageReceipt)
                .whenComplete(
                    (deleted, deleteException) -> {
                      if (deleteException != null) {
                        log.error(
                            "Exception while deleting message from queue={}",
                            queueName,
                            deleteException);
                        metrics.consumeFailure(deleteException, queueName);
                        ret.completeExceptionally(deleteException);
                      } else {
                        log.debug("Deleted message. queue={}", queueName);
                        long time = stopwatch.stop().elapsed(TimeUnit.NANOSECONDS);
                        metrics.consume(time, queueName);
                        log.debug("Consumed message in {} nanoseconds. queue={}.", time, queueName);
                        ret.complete(true);
                      }
                    });
          } else {
            metrics.consumeFailure(ex, queueName);
            log.error("Exception occurred while consuming message from queue.", ex);
            setMessageVisibility(queueName, messageReceipt);
            ret.completeExceptionally(ex);
          }
        });
  }

  private CompletableFuture<Boolean> consumeInternal(
      BiFunction<String, Executor, CompletableFuture<Void>> compute,
      @NonNull final String queueName) {
    bulkhead.acquirePermission();
    log.debug("Consuming message. queue={}.", queueName);
    Stopwatch stopwatch = Stopwatch.createStarted();
    return getMessage(queueName)
        .thenCompose(
            response -> {
              CompletableFuture<Boolean> ret = new CompletableFuture<>();

              Preconditions.checkState(
                  response.messages().size() <= 1, "Retrieved more than 1 message from SQS");

              if (response.messages().isEmpty()) {
                ret.complete(false);
              } else {
                Message message = response.messages().get(0);
                log.debug("Retrieved message. queue={}", queueName);

                CompletableFuture<Void> result = compute.apply(message.body(), executor);
                consumePostProcess(queueName, message.receiptHandle(), ret, result, stopwatch);
                log.info("Consumed message {} from queue {}", message.messageId(), queueName);
              }
              return ret;
            })
        .whenComplete((r, t) -> bulkhead.onComplete());
  }

  @Override
  public CompletableFuture<Void> add(
      @NonNull final String message, @NonNull final String queueName) {
    return add(message, queueName, 0);
  }

  @Override
  public CompletableFuture<Void> add(
      @NonNull final String message, @NonNull final String queueName, int delaySeconds) {
    log.debug("Adding message. queue={}", queueName);
    Stopwatch stopwatch = Stopwatch.createStarted();

    return urlCache
        .get(queueName)
        .thenCompose(
            url -> {
              SendMessageRequest request =
                  SendMessageRequest.builder()
                      .messageBody(message)
                      .queueUrl(url)
                      .delaySeconds(delaySeconds)
                      .build();
              return client.sendMessage(request);
            })
        .whenComplete(
            (response, ex) -> {
              if (response != null) {
                log.info(
                    "Published the message to SQS, id: {}, SequenceNumber: {}, QueueName: {}",
                    response.messageId(),
                    response.sequenceNumber(),
                    queueName);
                long time = stopwatch.stop().elapsed(TimeUnit.NANOSECONDS);
                metrics.add(time, queueName);
                log.debug("Added message in {} nanoseconds. queue={}.", time, queueName);
              } else {
                metrics.addFailure(ex, queueName);
                log.error("Failed to add message. queue={}.", queueName, ex);
              }
            })
        .thenApply(it -> null);
  }

  @Override
  public CompletableFuture<Boolean> consume(
      @NonNull final Consumer<String> consumer, @NonNull final String queueName) {
    return consumeInternal(
        (body, executor) -> CompletableFuture.runAsync(() -> consumer.accept(body), executor),
        queueName);
  }

  @Override
  public CompletableFuture<Boolean> consume(AsyncConsumer<String> consumer, String queueName) {
    return consumeInternal(consumer::accept, queueName);
  }

  @Override
  public void close() {
    client.close();
  }
}
