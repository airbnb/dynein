/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.config;

import java.net.URI;
import lombok.Value;

@Value
public class SQSConfiguration {
  private static final String DEFAULT_REGION = "us-east-1";

  private String region = DEFAULT_REGION;
  private URI endpoint;
  private TimeoutRegistry timeouts = new TimeoutRegistry();
  private String inboundQueueName;

  public URI getEndpoint() {
    return endpoint == null ? URI.create("https://sqs." + region + ".amazonaws.com") : endpoint;
  }
}
