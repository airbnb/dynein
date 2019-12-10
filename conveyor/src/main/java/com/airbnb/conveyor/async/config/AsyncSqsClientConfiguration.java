/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import javax.validation.constraints.Min;
import lombok.Data;
import lombok.NonNull;

@Data
public class AsyncSqsClientConfiguration {

  public static final long DEFAULT_URL_CACHE_SIZE = 128;
  public static final int DEFAULT_RECEIVE_WAIT_SECONDS = 5;
  public static final int DEFAULT_BULKHEAD_MAX_WAIT_MS = Integer.MAX_VALUE;
  public static final int DEFAULT_CONSUMER_CONCURRENCY = 10;
  public static final String DEFAULT_REGION = "us-east-1";
  public static final URI DEFAULT_ENDPOINT =
      URI.create("https://sqs." + DEFAULT_REGION + ".amazonaws.com");

  @JsonProperty(defaultValue = "production")
  @NonNull
  private final Type type;

  @JsonProperty("consumer_concurrency")
  @Min(1)
  private int consumerConcurrency = DEFAULT_CONSUMER_CONCURRENCY;

  @JsonProperty("bulkhead_max_wait_ms")
  @Min(0)
  private int bulkheadMaxWaitMillis = DEFAULT_BULKHEAD_MAX_WAIT_MS;

  @JsonProperty("receive_wait_seconds")
  @Min(0)
  private int receiveWaitSeconds = DEFAULT_RECEIVE_WAIT_SECONDS;

  @JsonProperty("max_url_cache_size")
  private long maxUrlCacheSize = DEFAULT_URL_CACHE_SIZE;

  @JsonProperty("region")
  private String region = DEFAULT_REGION;

  @JsonProperty("endpoint")
  private URI endpoint = DEFAULT_ENDPOINT;

  @JsonProperty("override_configuration")
  private OverrideConfiguration overrideConfiguration;

  public enum Type {
    PRODUCTION,
    MOCK;

    @JsonProperty
    public static Type create(String type) {
      return Type.valueOf(type.toUpperCase());
    }
  }
}
