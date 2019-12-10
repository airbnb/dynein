/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.worker;

import com.airbnb.conveyor.async.AsyncSqsClient;
import com.airbnb.dynein.api.DyneinJobSpec;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.utils.ExecutorUtils;
import com.airbnb.dynein.scheduler.Constants;
import com.airbnb.dynein.scheduler.Schedule;
import com.airbnb.dynein.scheduler.ScheduleManager;
import com.airbnb.dynein.scheduler.config.WorkersConfiguration;
import com.airbnb.dynein.scheduler.heartbeat.PartitionWorkerIterationEvent;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import com.google.common.base.Stopwatch;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PartitionWorker {
  private static final long EXECUTOR_TIMEOUT_SECONDS = 5;
  private final int index;

  @Named(Constants.SQS_PRODUCER)
  private final AsyncSqsClient asyncSqsClient;

  private final EventBus eventBus;
  private final Clock clock;
  private final ScheduleManager scheduleManager;
  private final JobSpecTransformer jobSpecTransformer;
  private final WorkersConfiguration workersConfiguration;
  private final Metrics metrics;

  private ScheduledExecutorService executorService = null;
  private ConcurrentHashMap<Integer, Long> scanTimes = null;

  CompletableFuture<Void> dispatchToDestination(Schedule schedule) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    CompletableFuture<Void> ret = new CompletableFuture<>();
    DyneinJobSpec jobSpec = jobSpecTransformer.deserializeJobSpec(schedule.getJobSpec());

    asyncSqsClient
        .add(schedule.getJobSpec(), jobSpec.getQueueName())
        .whenCompleteAsync(
            (it, ex) -> {
              if (ex != null) {
                log.error(
                    "Error dispatching job {} to destination queue {}",
                    jobSpec.getJobToken(),
                    jobSpec.getQueueName(),
                    ex);
                metrics.dispatchJobError(ex, jobSpec.getQueueName());
                scheduleManager
                    .updateStatus(
                        schedule, Schedule.JobStatus.ACQUIRED, Schedule.JobStatus.SCHEDULED)
                    .whenComplete((response, exception) -> ret.completeExceptionally(ex));
              } else {
                log.info(
                    "Dispatched job {} to destination queue {}",
                    jobSpec.getJobToken(),
                    jobSpec.getQueueName());
                long time = stopwatch.elapsed(TimeUnit.NANOSECONDS);
                metrics.dispatchScheduledJob(time, jobSpec, schedule.getShardId());
                scheduleManager
                    .deleteDispatchedJob(schedule)
                    .whenComplete((response, exception) -> ret.complete(null));
              }
            },
            executorService);

    return ret;
  }

  /*
   * dispatchOverdue: queries the DynamoDB for any overdue jobs in partition partition
   * returns: true if the number of overdue jobs returned is equal to queryLimit (configurable)
   * or if the query response hit its memory limit (1MB), signifying the need to query for overdue jobs
   * again immediately. Otherwise returns false, meaning that it should be safe to resume querying
   * after a short delay.
   */
  CompletableFuture<Boolean> dispatchOverdue(String partition) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    CompletableFuture<Boolean> ret = new CompletableFuture<>();
    scheduleManager
        .getOverdueJobs(partition)
        .whenCompleteAsync(
            (res, ex) -> {
              if (ex != null) {
                log.error("Error querying overdue jobs for partition {}", partition, ex);
                metrics.queryOverdueError(ex, partition);
                ret.complete(true);
              } else {
                metrics.queryOverdue(partition);
                CompletableFuture.allOf(
                        res.getSchedules()
                            .stream()
                            .map(
                                schedule ->
                                    scheduleManager
                                        .updateStatus(
                                            schedule,
                                            Schedule.JobStatus.SCHEDULED,
                                            Schedule.JobStatus.ACQUIRED)
                                        .thenCompose(this::dispatchToDestination))
                            .toArray(CompletableFuture[]::new))
                    .whenCompleteAsync(
                        (it, exception) -> {
                          // explicitly don't handle any exceptions here because:
                          // - acquire errors are harmless,
                          //   either CAS exception or will be picked up by the next iteration
                          // - dispatch errors are harmless,
                          //   - dispatch to SQS failures will be set back to ACQUIRED
                          //   - set back to ACQUIRED failures will be picked up by fix stuck jobs
                          ret.complete(res.isShouldImmediatelyQueryAgain());
                          long time = stopwatch.elapsed(TimeUnit.NANOSECONDS);
                          log.debug("Dispatched overdue jobs for partition {}", partition);
                          metrics.dispatchOverdue(time, partition);
                        },
                        executorService);
              }
            },
            executorService);
    return ret;
  }

  private void scanGroup(List<Integer> partitions) {
    if (executorService == null || executorService.isShutdown()) {
      log.info(
          "Partition worker has stopped scanning assigned partitions {}", partitions.toString());
      return;
    }

    boolean noDelay = false;
    // Stopwatch stopwatch = Stopwatch.createStarted();
    for (Integer partition : partitions) {
      Long now = Instant.now(clock).toEpochMilli();

      if (scanTimes.get(partition) < now) {
        boolean thisNoDelay =
            dispatchOverdue(partition.toString())
                .whenCompleteAsync(
                    (scheduleImmediate, exception) -> {
                      long nextScan = Instant.now(clock).toEpochMilli();
                      if (!scheduleImmediate) {
                        nextScan += 1000L;
                        log.debug(
                            "Next scan of partition {} delayed by a minimum of 1s", partition);
                      }
                      scanTimes.put(partition, nextScan);
                    },
                    executorService)
                .join();
        noDelay = noDelay || thisNoDelay;
      } else {
        // if a partition was skipped this iteration, we shouldn't delay next call to scanGroup()
        noDelay = true;
      }
    }
    eventBus.post(new PartitionWorkerIterationEvent(Instant.now(clock).toEpochMilli(), index));
    // long time = stopwatch.elapsed(TimeUnit.NANOSECONDS);
    log.debug("Iteration over partitions {} complete", partitions);

    // TODO(andy-fang): figure out what to do with this metric.
    // we likely still want to track this, but it currently causes datadog-agent OOM
    // metrics.iterationComplete(time);

    if (noDelay) {
      executorService.submit(() -> scanGroup(partitions));
    } else {
      log.info("Next iteration of partitions {} delayed by 1s", partitions);
      executorService.schedule(() -> scanGroup(partitions), 1000L, TimeUnit.MILLISECONDS);
    }
  }

  CompletableFuture<Void> recoverStuckJobs(List<Integer> partitions, long lookAheadMs) {
    log.info(
        "Recovering stuck jobs for partitions {} with lookAheadMs: {}", partitions, lookAheadMs);
    CompletableFuture<Void> allDone = CompletableFuture.completedFuture(null);
    for (int partition : partitions) {
      allDone =
          allDone
              .thenRun(
                  () ->
                      scheduleManager.recoverStuckJobs(
                          String.valueOf(partition), Instant.now(clock).plusMillis(lookAheadMs)))
              .exceptionally(
                  e -> {
                    log.error("Failed to recover stuck job for partition {}", partition, e);
                    metrics.recoverStuckJobsError(String.valueOf(partition));
                    return null; // suppress exceptions to unblock future actions
                  });
    }
    return allDone;
  }

  CompletableFuture<Void> recoverStuckJobs(List<Integer> partitions) {
    return recoverStuckJobs(partitions, workersConfiguration.getRecoverStuckJobsLookAheadMs());
  }

  void start(List<Integer> partitions) {
    long currentMillis = Instant.now(clock).toEpochMilli();
    startExecutor();
    partitions.forEach(partition -> scanTimes.put(partition, currentMillis));
    executorService.submit(
        () -> {
          log.info("Recovering stuck jobs during initial boot");
          recoverStuckJobs(partitions)
              .whenCompleteAsync((result, ex) -> scanGroup(partitions), executorService);
        });

    executorService.scheduleAtFixedRate(
        () -> {
          log.info(
              "Recovering stuck jobs at least {} ms old for partitions: {}",
              workersConfiguration.getRecoverStuckJobsLookAheadMs(),
              partitions);
          recoverStuckJobs(partitions, -workersConfiguration.getRecoverStuckJobsLookAheadMs())
              .join();
          log.info("Regular stuck job recovery done.");
        },
        workersConfiguration.getRecoverStuckJobsLookAheadMs(),
        workersConfiguration.getRecoverStuckJobsLookAheadMs(),
        TimeUnit.MILLISECONDS);
  }

  void shutdownExecutor() {
    if (executorService != null && !executorService.isShutdown()) {
      ExecutorUtils.twoPhaseExecutorShutdown(executorService, EXECUTOR_TIMEOUT_SECONDS);
    }
  }

  void startExecutor() {
    scanTimes = new ConcurrentHashMap<>();
    executorService =
        MoreExecutors.getExitingScheduledExecutorService(
            (ScheduledThreadPoolExecutor)
                Executors.newScheduledThreadPool(
                    workersConfiguration.getThreadPoolSize(),
                    new ThreadFactoryBuilder()
                        .setNameFormat("partition-worker-" + index + "-%s")
                        .build()));
    log.info("New ExecutorService has been created.");
  }
}
