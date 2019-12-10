/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class JobTokenPayload {
  @AllArgsConstructor
  public enum JobTokenType {
    SCHEDULED_JOB(0);
    int type;
  }

  private byte[] uuid;
  private short logicalShard;
  private String logicalCluster;
  private JobTokenType tokenType;
  private long epochMillis;

  @JsonIgnore
  public Optional<Long> getEpochMillisOptional() {
    return epochMillis == -1 ? Optional.empty() : Optional.of(epochMillis);
  }
}
