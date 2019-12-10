/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.dynamodb;

import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.scheduler.Constants;
import com.airbnb.dynein.scheduler.ScheduleManagerFactory;
import com.airbnb.dynein.scheduler.config.DynamoDBConfiguration;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import java.time.Clock;
import javax.inject.Inject;
import javax.inject.Named;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class DynamoDBScheduleManagerFactory extends ScheduleManagerFactory {
  private final DynamoDBConfiguration dynamoDBConfiguration;

  @Inject
  public DynamoDBScheduleManagerFactory(
      @Named(Constants.MAX_PARTITIONS) Integer maxShardId,
      TokenManager tokenManager,
      JobSpecTransformer jobSpecTransformer,
      Clock clock,
      Metrics metrics,
      DynamoDBConfiguration dynamoDBConfiguration) {
    super(maxShardId, tokenManager, jobSpecTransformer, clock, metrics);
    this.dynamoDBConfiguration = dynamoDBConfiguration;
  }

  private DynamoDbAsyncClient getDdbAsyncClient() {
    return DynamoDbAsyncClient.builder()
        .region(Region.of(dynamoDBConfiguration.getRegion()))
        .endpointOverride(dynamoDBConfiguration.getEndpoint())
        .build();
  }

  @Override
  public DynamoDBScheduleManager get() {
    return new DynamoDBScheduleManager(
        maxShardId,
        tokenManager,
        jobSpecTransformer,
        clock,
        metrics,
        getDdbAsyncClient(),
        dynamoDBConfiguration);
  }
}
