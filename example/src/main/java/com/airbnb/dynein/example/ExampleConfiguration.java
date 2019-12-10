/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.example;

import com.airbnb.dynein.scheduler.config.SchedulerConfiguration;
import io.dropwizard.Configuration;
import lombok.Data;

@Data
public class ExampleConfiguration extends Configuration {
  SchedulerConfiguration scheduler;
  String jobQueue;
}
