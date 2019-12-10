/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class RetryPolicyConfiguration {

  private static int DEFAULT_NUM_RETRIES = Integer.MAX_VALUE;
  private static int DEFAULT_BASE_DELAY = 100;
  private static int DEFAULT_MAX_BACKOFF_TIME = 10 * 1000;

  @JsonProperty("num_retries")
  private int numRetries = DEFAULT_NUM_RETRIES;

  @JsonProperty("base_delay")
  private int baseDelay = DEFAULT_BASE_DELAY;

  @JsonProperty("max_backoff_time")
  private int maximumBackoffTime = DEFAULT_MAX_BACKOFF_TIME;

  @JsonProperty("policy")
  private Policy policy = Policy.DEFAULT;

  @JsonProperty("condition")
  private Condition condition = Condition.DEFAULT;

  @JsonProperty("backoff")
  private BackOff backOff = BackOff.DEFAULT;

  public enum Policy {
    CUSTOM,
    DEFAULT,
    NONE;

    @JsonProperty
    public static Policy create(String policy) {
      return Policy.valueOf(policy.toUpperCase());
    }
  }

  public enum Condition {
    DEFAULT,
    NONE,
    MAX_NUM;

    @JsonProperty
    public static Condition create(String condition) {
      return Condition.valueOf(condition.toUpperCase());
    }
  }

  public enum BackOff {
    FULL_JITTER,
    EQUAL_JITTER,
    FIXED_DELAY,
    DEFAULT,
    DEFAULT_THROTTLE,
    NONE;

    @JsonProperty
    public static BackOff create(String strategy) {
      return BackOff.valueOf(strategy.toUpperCase());
    }
  }
}
