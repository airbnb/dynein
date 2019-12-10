/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.worker;

import com.airbnb.dynein.common.utils.ExecutorUtils;
import com.airbnb.dynein.scheduler.heartbeat.PartitionWorkerStalledEvent;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import com.airbnb.dynein.scheduler.partition.PartitionPolicy;
import com.google.common.base.Stopwatch;
import com.google.common.eventbus.Subscribe;
import io.dropwizard.lifecycle.Managed;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor = @__(@javax.inject.Inject))
public class WorkerManager implements Managed {

  private static final long EXECUTOR_TIMEOUT_SECONDS = 15;

  private final List<PartitionWorker> partitionWorkers;
  private final PartitionWorkerFactory partitionWorkerFactory;
  private final ScheduledExecutorService executorService;
  private ConcurrentHashMap<Integer, List<Integer>> workerIndexToPartitions;
  private final PartitionPolicy partitionPolicy;
  private final Metrics metrics;

  @Override
  public void start() {
    log.info("Attempting to start worker");
    executorService.scheduleWithFixedDelay(this::updatePartitions, 0, 30, TimeUnit.SECONDS);
    log.info("Started worker");
  }

  @Override
  public void stop() {
    log.info("Stopping worker");
    ExecutorUtils.twoPhaseExecutorShutdown(executorService, EXECUTOR_TIMEOUT_SECONDS);
    log.info("Stopped worker");
  }

  private void updatePartitions() {
    try {
      List<Integer> partitions = partitionPolicy.getPartitions();

      metrics.countPartitionWorkers(partitionWorkers.size());

      Set<Integer> currentPartitions =
          workerIndexToPartitions
              .values()
              .stream()
              .flatMap(List::stream)
              .collect(Collectors.toSet());
      boolean shouldRecreateWorkers = false;
      if (!currentPartitions.equals(new HashSet<>(partitions))) {
        log.info(
            "I'm working on: {}, previously I'm working on: {}", partitions, currentPartitions);
        shouldRecreateWorkers = true;
      }
      if (partitionWorkers.size() != workerIndexToPartitions.size()) {
        log.info(
            "Current number of workers: {} is different from the number of defined workers: {}",
            partitionWorkers.size(),
            workerIndexToPartitions.size());
        shouldRecreateWorkers = true;
      }
      if (shouldRecreateWorkers) {
        recreateWorkers(partitions);
      }
    } catch (Exception exception) {
      log.error("Unable to update partitions.", exception);
      metrics.updatePartitionsError(exception);
    }
  }

  private void recreateWorkers(List<Integer> partitions) {
    log.info("Recreate workers.");
    Stopwatch stopwatch = Stopwatch.createStarted();
    for (PartitionWorker partitionWorker : partitionWorkers) {
      partitionWorker.shutdownExecutor();
    }
    workerIndexToPartitions = new ConcurrentHashMap<>();
    for (int i = 0; i < partitionWorkers.size(); i++) {
      workerIndexToPartitions.put(i, new ArrayList<>());
    }
    for (int i = 0; i < partitions.size(); i++) {
      int partitionWorkerIndex = i % partitionWorkers.size();
      workerIndexToPartitions.get(partitionWorkerIndex).add(partitions.get(i));
    }

    log.info("Starting partition workers.");
    for (int i = 0; i < partitionWorkers.size(); i++) {
      List<Integer> thisWorkerPartitions = workerIndexToPartitions.get(i);
      if (!thisWorkerPartitions.isEmpty()) {
        partitionWorkers.get(i).start(thisWorkerPartitions);
      }
    }

    log.info("Partitions successfully updated.");
    long time = stopwatch.elapsed(TimeUnit.NANOSECONDS);
    metrics.updatePartitions(time);
    metrics.countPartitionWorkers(partitionWorkers.size());
  }

  @Subscribe
  public void restartStalledWorkers(PartitionWorkerStalledEvent stalledEvent) {
    stalledEvent
        .getStalledWorkers()
        .forEach(
            workerId -> {
              partitionWorkers.get(workerId).shutdownExecutor();
              PartitionWorker newWorker = partitionWorkerFactory.get(workerId);
              partitionWorkers.set(workerId, newWorker);
              newWorker.start(workerIndexToPartitions.get(workerId));
              log.info("Restarted PartitionWorker with index {}", workerId);
              metrics.restartWorker(workerId);
            });
    metrics.countPartitionWorkers(partitionWorkers.size());
  }
}
