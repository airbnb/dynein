/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class OverrideConfiguration {

  @JsonProperty("api_call_attempt_timeout")
  public int apiCallAttemptTimeout;

  @JsonProperty("api_call_timeout")
  public int apiCallTimeout;

  @JsonProperty("retry_policy_configuration")
  public RetryPolicyConfiguration retryPolicyConfiguration;
}
