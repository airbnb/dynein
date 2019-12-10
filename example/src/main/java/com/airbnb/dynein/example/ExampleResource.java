/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.example;

import com.airbnb.dynein.api.DyneinJobSpec;
import com.airbnb.dynein.api.JobSchedulePolicy;
import com.airbnb.dynein.api.JobScheduleType;
import com.airbnb.dynein.api.PrepareJobRequest;
import com.airbnb.dynein.example.api.ScheduleJobResponse;
import com.airbnb.dynein.scheduler.Scheduler;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import lombok.RequiredArgsConstructor;
import org.glassfish.jersey.server.ManagedAsync;

@Path("/schedule")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ExampleResource {
  private final Scheduler scheduler;
  private final Clock clock;
  private final ObjectMapper objectMapper;

  @Named("jobQueue")
  private final String jobQueue;

  private void scheduleJob(
      Optional<String> name,
      Optional<Integer> delaySeconds,
      JobScheduleType scheduleType,
      AsyncResponse asyncResponse) {
    try {
      Instant now = Instant.now(clock);
      JobSchedulePolicy policy =
          JobSchedulePolicy.builder()
              .type(scheduleType)
              .epochMillis(now.plusSeconds(delaySeconds.orElse(10)).toEpochMilli())
              .build();
      String token =
          scheduler.prepareJob(PrepareJobRequest.builder().schedulePolicy(policy).build());
      scheduler
          .createJob(
              DyneinJobSpec.builder()
                  .createAtInMillis(now.toEpochMilli())
                  .queueName(jobQueue)
                  .jobToken(token)
                  .name(ExampleJob.class.getName())
                  .serializedJob(objectMapper.writeValueAsBytes(name.orElse("default-name")))
                  .schedulePolicy(policy)
                  .build())
          .whenComplete(
              (response, e) -> {
                if (e != null) {
                  asyncResponse.resume(e);
                } else {
                  asyncResponse.resume(new ScheduleJobResponse(token));
                }
              });
    } catch (Exception e) {
      asyncResponse.resume(e);
    }
  }

  @GET
  @Path("/sqs-delayed")
  @Timed
  @ManagedAsync
  public void scheduleSqsDelayed(
      @QueryParam("name") Optional<String> name,
      @QueryParam("delaySeconds") Optional<Integer> delaySeconds,
      @Suspended final AsyncResponse asyncResponse) {
    scheduleJob(name, delaySeconds, JobScheduleType.SQS_DELAYED, asyncResponse);
  }

  @GET
  @Path("/immediate")
  @Timed
  @ManagedAsync
  public void scheduleImmediate(
      @QueryParam("name") Optional<String> name,
      @QueryParam("delaySeconds") Optional<Integer> delaySeconds,
      @Suspended final AsyncResponse asyncResponse) {
    scheduleJob(name, Optional.of(0), JobScheduleType.IMMEDIATE, asyncResponse);
  }

  @GET
  @Path("/scheduled")
  @Timed
  @ManagedAsync
  public void scheduleScheduled(
      @QueryParam("name") Optional<String> name,
      @QueryParam("delaySeconds") Optional<Integer> delaySeconds,
      @Suspended final AsyncResponse asyncResponse) {
    scheduleJob(name, delaySeconds, JobScheduleType.SCHEDULED, asyncResponse);
  }
}
