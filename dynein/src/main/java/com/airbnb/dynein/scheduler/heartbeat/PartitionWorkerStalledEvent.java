/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.heartbeat;

import java.util.List;
import lombok.Value;

@Value
public class PartitionWorkerStalledEvent {
  private List<Integer> stalledWorkers;
}
