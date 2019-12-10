/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler;

import com.airbnb.conveyor.async.AsyncConsumer;
import com.airbnb.conveyor.async.AsyncSqsClient;
import com.airbnb.dynein.api.InvalidTokenException;
import com.airbnb.dynein.common.utils.ExecutorUtils;
import io.dropwizard.lifecycle.Managed;
import java.util.concurrent.ExecutorService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class ManagedInboundJobQueueConsumer implements Managed {
  private static final long EXECUTOR_TIMEOUT_SECONDS = 15;
  private final AsyncSqsClient asyncClient;
  private final ExecutorService executorService;
  private final ScheduleManager scheduleManager;
  private final String inboundQueueName;

  @Override
  public void start() {
    log.info("Starting to consume from inbound job queue {}", inboundQueueName);

    executorService.execute(this::run);

    log.info("Started consuming from inbound job queue {}", inboundQueueName);
  }

  @Override
  public void stop() {
    log.info("Stopping consumption from inbound job queue {}", inboundQueueName);

    ExecutorUtils.twoPhaseExecutorShutdown(executorService, EXECUTOR_TIMEOUT_SECONDS);

    log.info("Stopped consumption from inbound job queue {}", inboundQueueName);
  }

  private void run() {
    while (!executorService.isShutdown()) {
      doRun();
    }
  }

  private void doRun() {
    AsyncConsumer<String> consumer =
        (message, executor) -> {
          try {
            return scheduleManager.addJob(scheduleManager.makeSchedule(message));
          } catch (InvalidTokenException e) {
            throw new RuntimeException(e);
          }
        };
    asyncClient
        .consume(consumer, inboundQueueName)
        .whenComplete(
            (it, ex) -> {
              if (ex != null) {
                log.error("Error consuming from inbound queue {}", inboundQueueName, ex);
              } else if (!it) {
                log.debug("Inbound job queue {} is empty", inboundQueueName);
              } else {
                log.info("Successfully consumed job from inbound queue");
              }
            });
  }
}
