/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Optional;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class JobSchedulePolicy {
  JobScheduleType type;
  @Builder.Default long delayMillis = -1L;
  @Builder.Default long epochMillis = -1L;

  @JsonIgnore
  public Optional<Long> getDelayMillisOptional() {
    return delayMillis == -1 ? Optional.empty() : Optional.of(delayMillis);
  }

  @JsonIgnore
  public Optional<Long> getEpochMillisOptional() {
    return epochMillis == -1 ? Optional.empty() : Optional.of(epochMillis);
  }
}
