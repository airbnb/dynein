/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.example.worker;

import com.airbnb.conveyor.async.AsyncConsumer;
import com.airbnb.conveyor.async.AsyncSqsClient;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.utils.ExecutorUtils;
import com.airbnb.dynein.example.ExampleJob;
import com.airbnb.dynein.scheduler.Constants;
import io.dropwizard.lifecycle.Managed;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public class ManagedWorker implements Managed {
  private static final long EXECUTOR_TIMEOUT_SECONDS = 15;

  @Named(Constants.SQS_CONSUMER)
  private final AsyncSqsClient asyncClient;

  @Named("workerExecutor")
  private final ExecutorService executorService;

  private final JobSpecTransformer transformer;

  @Named("jobQueue")
  private final String queueName;

  @Override
  public void start() {
    log.info("Starting to consume from job queue {}", queueName);

    executorService.execute(this::run);

    log.info("Started consuming from job queue {}", queueName);
  }

  @Override
  public void stop() {
    log.info("Stopping consumption from job queue {}", queueName);

    ExecutorUtils.twoPhaseExecutorShutdown(executorService, EXECUTOR_TIMEOUT_SECONDS);

    log.info("Stopped consumption from job queue {}", queueName);
  }

  private void run() {
    while (!executorService.isShutdown()) {
      doRun();
    }
  }

  private void doRun() {
    AsyncConsumer<String> consumer =
        (message, executor) ->
            CompletableFuture.runAsync(
                () ->
                    new ExampleJob(
                            new String(transformer.deserializeJobSpec(message).getSerializedJob()))
                        .run(),
                executor);
    asyncClient
        .consume(consumer, queueName)
        .whenComplete(
            (it, ex) -> {
              if (ex != null) {
                log.error("Error consuming from queue {}", queueName, ex);
              } else if (!it) {
                log.debug("Job queue {} is empty", queueName);
              } else {
                log.info("Successfully consumed job from queue");
              }
            });
  }
}
