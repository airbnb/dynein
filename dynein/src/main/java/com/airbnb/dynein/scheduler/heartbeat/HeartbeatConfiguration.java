/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.heartbeat;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class HeartbeatConfiguration {

  @JsonProperty("monitorInterval")
  private int monitorInterval = 1000;

  @JsonProperty("stallTolerance")
  private int stallTolerance = 60000;
}
