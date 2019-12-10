/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.config;

import java.util.List;
import lombok.Value;

@Value
public class WorkersConfiguration {
  public enum PartitionPolicyChoice {
    K8S,
    STATIC;
  }

  private int numberOfWorkers;
  private int threadPoolSize;
  private long recoverStuckJobsLookAheadMs;
  private boolean recoverStuckJobsAtWorkerRotation = true;
  private PartitionPolicyChoice partitionPolicy = PartitionPolicyChoice.K8S;
  private List<Integer> staticPartitionList = null;
}
