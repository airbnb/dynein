/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.config;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class TimeoutRegistry {

  // Execution timeout: total time allocated to the SQS client, include all of the retry attempts
  private int apiCallTimeout = 25 * 1000;

  // Request timeout: time allocated for each individual attempt
  private int apiCallAttemptTimeout = 2000;

  // Retry policy: scale factor for exponential backoff policy
  private int baseDelay = 100;

  // Retry policy: maximum backoff time for exponential backoff policy
  private int maxBackoffTime = 10 * 1000;
}
