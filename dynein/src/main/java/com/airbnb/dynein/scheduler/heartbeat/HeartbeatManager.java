/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.heartbeat;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.dropwizard.lifecycle.Managed;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor = @__(@Inject))
public class HeartbeatManager implements Managed {
  private static final long EXECUTOR_TIMEOUT_SECONDS = 15;
  private final ConcurrentHashMap<Integer, Long> checkIns;
  private final ScheduledExecutorService executorService;
  private final EventBus eventBus;
  private final Clock clock;
  private final HeartbeatConfiguration heartbeatConfiguration;

  @Override
  public void start() {
    log.info("Attempting to start heartbeat");

    executorService.scheduleWithFixedDelay(
        this::scanMap, 0, heartbeatConfiguration.getMonitorInterval(), TimeUnit.MILLISECONDS);

    log.info("Started heartbeat");
  }

  @Override
  public void stop() {
    log.info("Stopping heartbeat");

    executorService.shutdown();
    eventBus.unregister(this);

    try {
      if (!executorService.awaitTermination(EXECUTOR_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException ex) {
      throw new RuntimeException();
    }

    log.info("Stopped heartbeat");
  }

  @Subscribe
  public void workerCheckIn(PartitionWorkerIterationEvent iterationEvent) {
    log.debug(
        "Checking in index {} at time {}",
        iterationEvent.getIndex(),
        iterationEvent.getTimestamp());
    checkIns.put(iterationEvent.getIndex(), iterationEvent.getTimestamp());
  }

  private void scanMap() {
    long now = Instant.now(clock).toEpochMilli();
    List<Integer> stalled =
        checkIns
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue() < now - heartbeatConfiguration.getStallTolerance())
            .map(Entry::getKey)
            .collect(Collectors.toList());
    if (stalled.size() > 0) {
      eventBus.post(new PartitionWorkerStalledEvent(stalled));
    }
    log.debug("Completed scanning PartitionWorker check in times.");
  }
}
