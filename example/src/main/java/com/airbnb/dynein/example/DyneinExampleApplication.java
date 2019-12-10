/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.example;

import com.airbnb.dynein.example.worker.ManagedWorker;
import com.airbnb.dynein.scheduler.ManagedInboundJobQueueConsumer;
import com.airbnb.dynein.scheduler.modules.SchedulerModule;
import com.airbnb.dynein.scheduler.worker.WorkerManager;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class DyneinExampleApplication extends Application<ExampleConfiguration> {
  public static void main(String[] args) throws Exception {
    new DyneinExampleApplication().run(args);
  }

  @Override
  public String getName() {
    return "hello-world";
  }

  @Override
  public void initialize(Bootstrap<ExampleConfiguration> bootstrap) {}

  @Override
  public void run(ExampleConfiguration configuration, Environment environment) {
    Injector injector =
        Guice.createInjector(
            new SchedulerModule(configuration.getScheduler()), new ExampleModule(configuration));
    final ExampleResource resource = injector.getInstance(ExampleResource.class);
    environment.jersey().register(resource);
    // worker that executes the jobs
    environment.lifecycle().manage(injector.getInstance(ManagedWorker.class));
    // worker that fetches jobs from inbound queue
    environment.lifecycle().manage(injector.getInstance(ManagedInboundJobQueueConsumer.class));
    // worker that executes schedules from dynamodb
    environment.lifecycle().manage(injector.getInstance(WorkerManager.class));
  }
}
