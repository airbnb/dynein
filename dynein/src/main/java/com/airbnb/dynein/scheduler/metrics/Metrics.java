/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.metrics;

import com.airbnb.dynein.api.DyneinJobSpec;

public interface Metrics {
  default void storeJob(final long elapsedTime, DyneinJobSpec jobSpec, String partition) {}

  default void storeJobError(Throwable throwable, String queueName) {}

  default void queryOverdue(String partition) {}

  default void queryOverdueError(Throwable throwable, String partition) {}

  default void updateJobStatus(String oldStatus, String newStatus) {}

  default void updateJobStatusError(Throwable throwable, String oldStatus, String newStatus) {}

  default void deleteDispatchedJob(String queueName) {}

  default void deleteDispatchedJobError(Throwable throwable, String queueName) {}

  default void dispatchJob(String queueName) {}

  default void dispatchScheduledJob(long timeNs, DyneinJobSpec jobSpec, String partition) {}

  default void dispatchJobError(Throwable t, String queueName) {}

  default void enqueueToInboundQueue(DyneinJobSpec jobSpec) {}

  default void dispatchOverdue(long timeNs, String partition) {}

  default void updatePartitions(long timeNs) {}

  default void updatePartitionsError(Throwable t) {}

  default void countPartitionWorkers(int count) {}

  default void restartWorker(int workerId) {}

  default void recoverStuckJob(String partition, int successCount, int totalCount) {}

  default void recoverStuckJobsError(String partition) {}
}
