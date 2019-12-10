/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler;

import com.airbnb.dynein.api.*;
import com.airbnb.dynein.common.job.JacksonJobSpecTransformer;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.JacksonTokenManager;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.scheduler.Schedule.JobStatus;
import com.airbnb.dynein.scheduler.metrics.NoOpMetricsImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchedulerManagerTest {
  private static final byte[] SERIALIZED_JOB_DATA = {0, 0, 0, 0};
  private JobSpecTransformer transformer;
  private TokenManager tokenManager;
  private ScheduleManager scheduleManager;
  private Clock clock;

  @Before
  public void setUp() {
    ObjectMapper mapper = new ObjectMapper();
    transformer = new JacksonJobSpecTransformer(mapper);
    tokenManager = new JacksonTokenManager(mapper);
    clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"));
    int maxShardId = 64;
    scheduleManager =
        new NoOpScheduleManager(
            maxShardId, tokenManager, transformer, clock, new NoOpMetricsImpl());
  }

  private DyneinJobSpec getTestJobSpec(String token) {
    JobSchedulePolicy policy =
        JobSchedulePolicy.builder()
            .type(JobScheduleType.SCHEDULED)
            .epochMillis(Instant.now(clock).plusSeconds(1000).toEpochMilli())
            .build();
    return DyneinJobSpec.builder()
        .jobToken(token)
        .name("AddJob")
        .queueType("PRODUCTION")
        .queueName("test-queue")
        .createAtInMillis(Instant.now().minusMillis(10).toEpochMilli())
        .schedulePolicy(policy)
        .serializedJob(SERIALIZED_JOB_DATA)
        .build();
  }

  /**
   * This test is to ensure that we always use the scheduled time in the jobSpec to make the {@code
   * Schedule} rather than the one in the token.
   */
  @Test
  public void testMakeSchedule() throws InvalidTokenException {
    String token =
        tokenManager.generateToken(1L, "test-cluster", Instant.now(clock).toEpochMilli());
    String serializedJobSpec = transformer.serializeJobSpec(getTestJobSpec(token));
    Schedule schedule = scheduleManager.makeSchedule(serializedJobSpec);
    Assert.assertEquals(
        schedule,
        new Schedule(
            Instant.now(clock).plusSeconds(1000).toEpochMilli() + "#" + token,
            JobStatus.SCHEDULED,
            serializedJobSpec,
            "1"));
  }
}
