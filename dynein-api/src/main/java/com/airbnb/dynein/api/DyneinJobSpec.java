/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.api;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DyneinJobSpec {
  JobSchedulePolicy schedulePolicy;
  long createAtInMillis;
  String queueName;
  String queueType;
  String jobToken;
  String name;
  byte[] serializedJob;
}
