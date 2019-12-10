/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.airbnb.conveyor.async.config.AsyncSqsClientConfiguration;
import com.airbnb.conveyor.async.config.OverrideConfiguration;
import com.airbnb.conveyor.async.config.RetryPolicyConfiguration;
import com.airbnb.conveyor.async.metrics.NoOpConveyorMetrics;
import com.airbnb.conveyor.async.mock.MockAsyncSqsClient;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class AsyncSqsClientFactoryTest {
  private static final AsyncSqsClientConfiguration ASYNC_CONFIG =
      new AsyncSqsClientConfiguration(AsyncSqsClientConfiguration.Type.PRODUCTION);

  private static final AsyncSqsClientConfiguration LOCAL_CONFIG =
      new AsyncSqsClientConfiguration(AsyncSqsClientConfiguration.Type.MOCK);

  private final AsyncSqsClientFactory factory =
      new AsyncSqsClientFactory(new NoOpConveyorMetrics());

  @Test
  public void testLocal() throws Exception {
    AsyncSqsClient localQueue = factory.create(LOCAL_CONFIG);
    assertTrue(localQueue instanceof MockAsyncSqsClient);
  }

  @Test
  public void testAsync() throws Exception {
    AsyncSqsClient asyncClient = factory.create(ASYNC_CONFIG);
    assertTrue(asyncClient instanceof AsyncSqsClientImpl);

    SqsAsyncClient sqs = ((AsyncSqsClientImpl) asyncClient).getClient();
    assertNotNull(sqs);
  }

  @Test
  public void testAsyncWithOverrideConfig() throws Exception {
    AsyncSqsClientConfiguration config =
        new AsyncSqsClientConfiguration(AsyncSqsClientConfiguration.Type.PRODUCTION);

    RetryPolicyConfiguration retryPolicyConfiguration = new RetryPolicyConfiguration();
    retryPolicyConfiguration.setCondition(RetryPolicyConfiguration.Condition.DEFAULT);
    retryPolicyConfiguration.setBackOff(RetryPolicyConfiguration.BackOff.EQUAL_JITTER);

    OverrideConfiguration overrideConfiguration = new OverrideConfiguration();
    overrideConfiguration.setApiCallAttemptTimeout(300);
    overrideConfiguration.setApiCallTimeout(25 * 1000);
    overrideConfiguration.setRetryPolicyConfiguration(retryPolicyConfiguration);

    config.setOverrideConfiguration(overrideConfiguration);

    AsyncSqsClient asyncClient = factory.create(config);
    assertTrue(asyncClient instanceof AsyncSqsClientImpl);

    SqsAsyncClient sqs = ((AsyncSqsClientImpl) asyncClient).getClient();
    assertNotNull(sqs);
  }
}
