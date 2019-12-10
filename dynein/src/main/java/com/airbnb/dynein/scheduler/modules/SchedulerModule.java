/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.modules;

import com.airbnb.conveyor.async.AsyncSqsClient;
import com.airbnb.conveyor.async.AsyncSqsClientFactory;
import com.airbnb.conveyor.async.config.AsyncSqsClientConfiguration;
import com.airbnb.conveyor.async.config.OverrideConfiguration;
import com.airbnb.conveyor.async.config.RetryPolicyConfiguration;
import com.airbnb.conveyor.async.metrics.AsyncConveyorMetrics;
import com.airbnb.conveyor.async.metrics.NoOpConveyorMetrics;
import com.airbnb.dynein.common.job.JacksonJobSpecTransformer;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.JacksonTokenManager;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.scheduler.Constants;
import com.airbnb.dynein.scheduler.ManagedInboundJobQueueConsumer;
import com.airbnb.dynein.scheduler.ScheduleManager;
import com.airbnb.dynein.scheduler.ScheduleManagerFactory;
import com.airbnb.dynein.scheduler.config.DynamoDBConfiguration;
import com.airbnb.dynein.scheduler.config.SQSConfiguration;
import com.airbnb.dynein.scheduler.config.SchedulerConfiguration;
import com.airbnb.dynein.scheduler.config.WorkersConfiguration;
import com.airbnb.dynein.scheduler.dynamodb.DynamoDBScheduleManagerFactory;
import com.airbnb.dynein.scheduler.heartbeat.HeartbeatConfiguration;
import com.airbnb.dynein.scheduler.heartbeat.HeartbeatManager;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import com.airbnb.dynein.scheduler.metrics.NoOpMetricsImpl;
import com.airbnb.dynein.scheduler.partition.K8SConsecutiveAllocationPolicy;
import com.airbnb.dynein.scheduler.partition.PartitionPolicy;
import com.airbnb.dynein.scheduler.partition.StaticAllocationPolicy;
import com.airbnb.dynein.scheduler.worker.PartitionWorker;
import com.airbnb.dynein.scheduler.worker.PartitionWorkerFactory;
import com.airbnb.dynein.scheduler.worker.WorkerManager;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

@Slf4j
public class SchedulerModule extends AbstractModule {
  private final SchedulerConfiguration schedulerConfiguration;
  private final DynamoDBConfiguration dynamoDBConfiguration;
  private final SQSConfiguration sqsConfiguration;
  private final HeartbeatConfiguration heartbeatConfiguration;
  private final WorkersConfiguration workersConfiguration;

  public SchedulerModule(SchedulerConfiguration schedulerConfiguration) {
    this.schedulerConfiguration = schedulerConfiguration;
    this.dynamoDBConfiguration = schedulerConfiguration.getDynamoDb();
    this.sqsConfiguration = schedulerConfiguration.getSqs();
    this.heartbeatConfiguration = schedulerConfiguration.getHeartbeat();
    this.workersConfiguration = schedulerConfiguration.getWorkers();
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(WorkersConfiguration.class).toInstance(workersConfiguration);
    bind(DynamoDBConfiguration.class).toInstance(dynamoDBConfiguration);
    bind(Integer.class)
        .annotatedWith(Names.named(Constants.MAX_PARTITIONS))
        .toInstance(schedulerConfiguration.getMaxPartitions());
    bind(String.class)
        .annotatedWith(Names.named(Constants.INBOUND_QUEUE_NAME))
        .toInstance(sqsConfiguration.getInboundQueueName());

    bind(DynamoDBScheduleManagerFactory.class).asEagerSingleton();
    bind(ScheduleManagerFactory.class).to(DynamoDBScheduleManagerFactory.class).asEagerSingleton();
    bind(ScheduleManager.class).toProvider(ScheduleManagerFactory.class).asEagerSingleton();
    bind(EventBus.class).toInstance(new EventBus());

    switch (workersConfiguration.getPartitionPolicy()) {
      case K8S:
        {
          bind(K8SConsecutiveAllocationPolicy.class).asEagerSingleton();
          bind(PartitionPolicy.class).to(K8SConsecutiveAllocationPolicy.class);
          break;
        }
      case STATIC:
        {
          bind(PartitionPolicy.class)
              .toInstance(
                  new StaticAllocationPolicy(workersConfiguration.getStaticPartitionList()));
          break;
        }
    }
    bind(Metrics.class).to(NoOpMetricsImpl.class).asEagerSingleton();
    bind(AsyncConveyorMetrics.class).to(NoOpConveyorMetrics.class).asEagerSingleton();
    bind(JobSpecTransformer.class).to(JacksonJobSpecTransformer.class).asEagerSingleton();
    bind(TokenManager.class).to(JacksonTokenManager.class).asEagerSingleton();
  }

  @Provides
  public DynamoDbAsyncClient providesDdbAsyncClient() {
    return DynamoDbAsyncClient.builder()
        .endpointOverride(dynamoDBConfiguration.getEndpoint())
        .build();
  }

  @Provides
  @Singleton
  public ManagedInboundJobQueueConsumer providesManagedInboundJobQueueConsumer(
      @Named(Constants.SQS_CONSUMER) AsyncSqsClient asyncClient, ScheduleManager scheduleManager) {
    return new ManagedInboundJobQueueConsumer(
        asyncClient,
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor)
                Executors.newFixedThreadPool(
                    1, new ThreadFactoryBuilder().setNameFormat("inbound-consumer-%d").build())),
        scheduleManager,
        sqsConfiguration.getInboundQueueName());
  }

  private AsyncSqsClientConfiguration buildBaseSqsConfig() {
    AsyncSqsClientConfiguration asyncSqsConfig =
        new AsyncSqsClientConfiguration(AsyncSqsClientConfiguration.Type.PRODUCTION);

    asyncSqsConfig.setRegion(sqsConfiguration.getRegion());
    asyncSqsConfig.setEndpoint(sqsConfiguration.getEndpoint());
    return asyncSqsConfig;
  }

  @Provides
  @Named(Constants.SQS_CONSUMER)
  AsyncSqsClientConfiguration provideConsumerSqsConfig() {
    return buildBaseSqsConfig();
  }

  @Provides
  @Named(Constants.SQS_PRODUCER)
  AsyncSqsClientConfiguration provideProducerSqsConfig() {
    AsyncSqsClientConfiguration asyncSqsConfig = buildBaseSqsConfig();

    RetryPolicyConfiguration retryConfig = new RetryPolicyConfiguration();
    retryConfig.setPolicy(RetryPolicyConfiguration.Policy.CUSTOM);
    retryConfig.setCondition(RetryPolicyConfiguration.Condition.DEFAULT);
    retryConfig.setBackOff(RetryPolicyConfiguration.BackOff.EQUAL_JITTER);
    retryConfig.setMaximumBackoffTime(sqsConfiguration.getTimeouts().getMaxBackoffTime());
    retryConfig.setBaseDelay(sqsConfiguration.getTimeouts().getBaseDelay());

    OverrideConfiguration overrideConfig = new OverrideConfiguration();
    overrideConfig.setApiCallAttemptTimeout(
        sqsConfiguration.getTimeouts().getApiCallAttemptTimeout());
    overrideConfig.setApiCallTimeout(sqsConfiguration.getTimeouts().getApiCallTimeout());
    overrideConfig.setRetryPolicyConfiguration(retryConfig);
    asyncSqsConfig.setOverrideConfiguration(overrideConfig);
    return asyncSqsConfig;
  }

  @Provides
  @Singleton
  @Named(Constants.SQS_CONSUMER)
  public AsyncSqsClient provideAsyncConsumer(
      AsyncSqsClientFactory asyncSqsClientFactory,
      @Named(Constants.SQS_CONSUMER) AsyncSqsClientConfiguration asyncSqsClientConfiguration) {
    return asyncSqsClientFactory.create(asyncSqsClientConfiguration);
  }

  @Provides
  @Singleton
  @Named(Constants.SQS_PRODUCER)
  public AsyncSqsClient provideAsyncProducer(
      AsyncSqsClientFactory asyncSqsClientFactory,
      @Named(Constants.SQS_PRODUCER) AsyncSqsClientConfiguration asyncSqsClientConfiguration) {
    return asyncSqsClientFactory.create(asyncSqsClientConfiguration);
  }

  @Provides
  @Singleton
  public HeartbeatManager provideHeartBeat(EventBus eventBus, Clock clock) {
    HeartbeatManager heartbeatManager =
        new HeartbeatManager(
            new ConcurrentHashMap<>(),
            MoreExecutors.getExitingScheduledExecutorService(
                (ScheduledThreadPoolExecutor)
                    Executors.newScheduledThreadPool(
                        1,
                        new ThreadFactoryBuilder().setNameFormat("heartbeat-manager-%d").build())),
            eventBus,
            clock,
            heartbeatConfiguration);
    eventBus.register(heartbeatManager);
    return heartbeatManager;
  }

  @Provides
  @Singleton
  public WorkerManager provideWorkerManager(
      PartitionWorkerFactory factory, EventBus eventBus, Metrics metrics, PartitionPolicy policy) {

    List<PartitionWorker> partitionWorkers = new ArrayList<>();
    for (int i = 0; i < workersConfiguration.getNumberOfWorkers(); i++) {
      partitionWorkers.add(factory.get(i));
    }

    WorkerManager manager =
        new WorkerManager(
            partitionWorkers,
            factory,
            MoreExecutors.getExitingScheduledExecutorService(
                (ScheduledThreadPoolExecutor)
                    Executors.newScheduledThreadPool(
                        1, new ThreadFactoryBuilder().setNameFormat("worker-manager-%d").build())),
            new ConcurrentHashMap<>(),
            policy,
            metrics);

    eventBus.register(manager);
    return manager;
  }
}
