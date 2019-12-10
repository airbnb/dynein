/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.partition;

import java.util.List;

public interface PartitionPolicy {
  List<Integer> getPartitions();
}
