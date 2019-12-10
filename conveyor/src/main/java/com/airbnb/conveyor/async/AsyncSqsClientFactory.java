/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async;

import com.airbnb.conveyor.async.config.AsyncSqsClientConfiguration;
import com.airbnb.conveyor.async.config.OverrideConfiguration;
import com.airbnb.conveyor.async.config.RetryPolicyConfiguration;
import com.airbnb.conveyor.async.metrics.AsyncConveyorMetrics;
import com.airbnb.conveyor.async.mock.MockAsyncSqsClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.concurrent.*;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.MaxNumberOfRetriesCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

/** Responsible for creating {@code AsyncClient}s from configuration */
@Slf4j
@AllArgsConstructor(onConstructor = @__(@Inject))
public class AsyncSqsClientFactory {

  @NonNull private final AsyncConveyorMetrics metrics;

  /**
   * Provides a {@code AsyncClient} given the specified configuration.
   *
   * @param configuration The {@code AsyncClientConfiguration}.
   * @return the corresponding {@code AsyncClient}.
   */
  public AsyncSqsClient create(@NonNull final AsyncSqsClientConfiguration configuration) {
    return createClient(configuration);
  }

  private RetryPolicy buildRetryPolicy(RetryPolicyConfiguration config) {
    RetryPolicy retryPolicy;

    if (config == null) {
      retryPolicy = RetryPolicy.none();
    } else {
      switch (config.getPolicy()) {
        case DEFAULT:
          retryPolicy = RetryPolicy.defaultRetryPolicy();
          break;
        case NONE:
          retryPolicy = RetryPolicy.none();
          break;
        default:
          RetryCondition condition;
          BackoffStrategy strategy;

          switch (config.getCondition()) {
            case DEFAULT:
              condition = RetryCondition.defaultRetryCondition();
              break;
            case MAX_NUM:
              condition = MaxNumberOfRetriesCondition.create(config.getNumRetries());
              break;
            default:
              condition = RetryCondition.none();
          }

          switch (config.getBackOff()) {
            case FULL_JITTER:
              strategy =
                  FullJitterBackoffStrategy.builder()
                      .baseDelay(Duration.ofMillis(config.getBaseDelay()))
                      .maxBackoffTime(Duration.ofMillis(config.getMaximumBackoffTime()))
                      .build();
              break;
            case EQUAL_JITTER:
              strategy =
                  EqualJitterBackoffStrategy.builder()
                      .baseDelay(Duration.ofMillis(config.getBaseDelay()))
                      .maxBackoffTime(Duration.ofMillis(config.getMaximumBackoffTime()))
                      .build();
              break;
            case FIXED_DELAY:
              strategy = FixedDelayBackoffStrategy.create(Duration.ofMillis(config.getBaseDelay()));
              break;
            case DEFAULT:
              strategy = BackoffStrategy.defaultStrategy();
              break;
            case DEFAULT_THROTTLE:
              strategy = BackoffStrategy.defaultThrottlingStrategy();
              break;
            default:
              strategy = BackoffStrategy.none();
          }

          retryPolicy =
              RetryPolicy.builder()
                  .numRetries(config.getNumRetries())
                  .retryCondition(condition)
                  .backoffStrategy(strategy)
                  .build();
      }
    }

    return retryPolicy;
  }

  private SqsAsyncClient getAsyncSQSClient(AsyncSqsClientConfiguration config) {
    if (config.getType().equals(AsyncSqsClientConfiguration.Type.PRODUCTION)) {
      SqsAsyncClientBuilder builder =
          SqsAsyncClient.builder()
              .region(Region.of(config.getRegion()))
              .endpointOverride(config.getEndpoint());

      OverrideConfiguration overrideConfig = config.getOverrideConfiguration();

      if (overrideConfig != null) {

        RetryPolicy retry = buildRetryPolicy(overrideConfig.getRetryPolicyConfiguration());
        ClientOverrideConfiguration clientOverrideConfiguration =
            ClientOverrideConfiguration.builder()
                .apiCallAttemptTimeout(Duration.ofMillis(overrideConfig.getApiCallAttemptTimeout()))
                .apiCallTimeout(Duration.ofMillis(overrideConfig.getApiCallTimeout()))
                .retryPolicy(retry)
                .build();

        builder = builder.overrideConfiguration(clientOverrideConfiguration);
      }
      return builder.build();
    } else {
      throw new IllegalArgumentException("Doesn't support this Type " + config.getType());
    }
  }

  private AsyncSqsClient createClient(@NonNull final AsyncSqsClientConfiguration configuration) {
    if (configuration.getType().equals(AsyncSqsClientConfiguration.Type.MOCK)) {
      return new MockAsyncSqsClient();
    } else {
      SqsAsyncClient asyncSqsClient = getAsyncSQSClient(configuration);
      return new AsyncSqsClientImpl(
          asyncSqsClient,
          metrics,
          MoreExecutors.getExitingExecutorService(
              new ThreadPoolExecutor(
                  configuration.getConsumerConcurrency(),
                  configuration.getConsumerConcurrency(),
                  0L,
                  TimeUnit.MILLISECONDS,
                  new LinkedBlockingQueue<>(),
                  new ThreadFactoryBuilder().setNameFormat("conveyor-executor-%d").build())),
          configuration.getMaxUrlCacheSize(),
          configuration.getReceiveWaitSeconds(),
          configuration.getBulkheadMaxWaitMillis(),
          configuration.getConsumerConcurrency());
    }
  }
}
