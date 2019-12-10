/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async;

import static org.junit.Assert.*;

import com.airbnb.conveyor.async.mock.MockAsyncSqsClient;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

public class MockAsyncSqsClientTest {
  private static final String QUEUE_NAME = "test";

  @Test
  public void testAdd() {
    AsyncSqsClient queue = new MockAsyncSqsClient();
    Set<String> messages = ImmutableSet.of("hello", "there");

    messages.forEach((msg) -> queue.add(msg, QUEUE_NAME));

    Set<String> receivedMessages = new HashSet<>();

    queue.consume(receivedMessages::add, QUEUE_NAME).join();
    queue.consume(receivedMessages::add, QUEUE_NAME).join();

    assertEquals(messages, receivedMessages);
  }

  @Test
  public void testAddWithDelay() {
    AsyncSqsClient queue = new MockAsyncSqsClient();

    Set<String> messages = ImmutableSet.of("hello");
    queue.add("hello", QUEUE_NAME, 5).join();

    Set<String> receivedMessages = new HashSet<>();

    assertTrue(queue.consume(receivedMessages::add, QUEUE_NAME).join());
    assertEquals(messages, receivedMessages);
  }

  @Test
  public void testAsyncConsume() {
    AsyncSqsClient queue = new MockAsyncSqsClient();
    Set<String> messages = ImmutableSet.of("hello", "there");

    messages.forEach((msg) -> queue.add(msg, QUEUE_NAME));

    AsyncConsumer<String> function = (str, threadpool) -> CompletableFuture.completedFuture(null);
    CompletableFuture<Boolean> result = queue.consume(function, QUEUE_NAME);

    // testing final result
    assertTrue(result.join());
  }

  @Test
  public void testConsumeWithEmptyQueue() {
    MockAsyncSqsClient emptyQueue = new MockAsyncSqsClient();
    AsyncConsumer<String> function = (str, threadpool) -> CompletableFuture.completedFuture(null);
    CompletableFuture<Boolean> result = emptyQueue.consume(function, "empty");

    // testing final result
    assertFalse(result.join());
  }

  @Test(expected = Exception.class)
  public void testConsumeCompletion() {
    AsyncSqsClient queue = new MockAsyncSqsClient();
    Set<String> messages = ImmutableSet.of("hello", "there");

    messages.forEach((msg) -> queue.add(msg, QUEUE_NAME));

    AsyncConsumer<String> function =
        (str, threadpool) -> {
          CompletableFuture<Void> ret = new CompletableFuture<>();
          ret.completeExceptionally(new Exception());
          return ret;
        };
    CompletableFuture<Boolean> result = queue.consume(function, QUEUE_NAME);

    // testing final result
    result.join();
  }
}
