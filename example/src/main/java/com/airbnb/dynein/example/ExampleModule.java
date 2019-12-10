/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.example;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ExampleModule extends AbstractModule {
  private final ExampleConfiguration configuration;

  @Override
  protected void configure() {
    bind(String.class)
        .annotatedWith(Names.named("jobQueue"))
        .toInstance(configuration.getJobQueue());
    bind(ExecutorService.class)
        .annotatedWith(Names.named("workerExecutor"))
        .toInstance(Executors.newSingleThreadExecutor());
  }
}
