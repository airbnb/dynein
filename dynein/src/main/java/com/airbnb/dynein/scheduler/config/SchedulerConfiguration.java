/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.config;

import com.airbnb.dynein.scheduler.heartbeat.HeartbeatConfiguration;
import javax.validation.constraints.NotNull;
import lombok.Value;

@Value
public class SchedulerConfiguration {
  @NotNull private WorkersConfiguration workers;
  @NotNull private HeartbeatConfiguration heartbeat;
  @NotNull private SQSConfiguration sqs;
  @NotNull private DynamoDBConfiguration dynamoDb;
  private int maxPartitions;
  private boolean consumeInboundJobs;
}
