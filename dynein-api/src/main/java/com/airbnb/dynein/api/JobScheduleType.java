/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.api;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum JobScheduleType {
  IMMEDIATE(1),
  SCHEDULED(2),
  SQS_DELAYED(3);
  int type;
}
