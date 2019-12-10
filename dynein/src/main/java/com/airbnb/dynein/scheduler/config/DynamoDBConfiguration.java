/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.config;

import java.net.URI;
import java.util.Optional;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DynamoDBConfiguration {
  public static final String DEFAULT_REGION = "us-east-1";
  public static final String DEFAULT_TABLE_NAME = "job_schedules";

  private String region = DEFAULT_REGION;
  private URI endpoint = null;

  private String schedulesTableName = DEFAULT_TABLE_NAME;
  private int queryLimit = 10;

  public URI getEndpoint() {
    return Optional.ofNullable(endpoint)
        .orElseGet(() -> URI.create("https://dynamodb." + region + ".amazonaws.com"));
  }
}
