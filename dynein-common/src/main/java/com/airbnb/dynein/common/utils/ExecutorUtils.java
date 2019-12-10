/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.common.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class ExecutorUtils {
  // adapted from the example in
  // https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
  public void twoPhaseExecutorShutdown(ExecutorService executor, long timeoutSeconds) {
    log.info("attempting to shutdown executor {}", executor);
    executor.shutdown(); // reject new task submission
    try {
      if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
        executor.shutdownNow(); // cancel currently running tasks
        if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
          log.error("Executor did not terminate");
        }
      }
    } catch (InterruptedException ex) {
      log.warn("interrupted while attempting to shutdown {}", executor);
      // (Re-)Cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
    log.info("successfully shutdown executor {}", executor);
  }
}
