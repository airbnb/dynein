/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Optional;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PrepareJobRequest {
  JobSchedulePolicy schedulePolicy;
  @Builder.Default long associatedId = -1;

  @JsonIgnore
  public Optional<JobSchedulePolicy> getSchedulePolicyOptional() {
    return Optional.ofNullable(schedulePolicy);
  }
}
