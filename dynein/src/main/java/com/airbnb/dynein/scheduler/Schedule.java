/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.Wither;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Value
@AllArgsConstructor
@Wither
public class Schedule {
  private final String dateToken;
  private final JobStatus status;
  private final String jobSpec;
  private final String shardId;

  public enum JobStatus {
    SCHEDULED,
    ACQUIRED
  }
}
