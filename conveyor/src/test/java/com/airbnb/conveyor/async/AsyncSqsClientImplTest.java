/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.conveyor.async;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.airbnb.conveyor.async.config.AsyncSqsClientConfiguration;
import com.airbnb.conveyor.async.metrics.AsyncConveyorMetrics;
import com.airbnb.conveyor.async.metrics.NoOpConveyorMetrics;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

public class AsyncSqsClientImplTest {
  private AsyncSqsClient asyncClient;
  private SqsAsyncClient awsAsyncSqsClient;
  private ArgumentCaptor<SendMessageRequest> sentRequest;
  private ThreadPoolExecutor executor;
  private AsyncConveyorMetrics metrics;

  @Before
  public void setUp() {
    this.awsAsyncSqsClient = Mockito.mock(SqsAsyncClient.class);
    metrics = new NoOpConveyorMetrics();
    executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    asyncClient = new AsyncSqsClientImpl(awsAsyncSqsClient, metrics, executor);
  }

  // dummy add to queue
  private void queueAddSetup() {
    CompletableFuture<SendMessageResponse> messageResponse =
        CompletableFuture.completedFuture(
            SendMessageResponse.builder()
                .messageId("testId")
                .sequenceNumber("testSeqNumber")
                .build());
    when(awsAsyncSqsClient.sendMessage(any(SendMessageRequest.class))).thenReturn(messageResponse);
  }

  // dummy get from queue
  private void receiveSetup() {
    Message message = Message.builder().body("testBody").receiptHandle("testHandle").build();
    CompletableFuture<ReceiveMessageResponse> receiveMessage =
        CompletableFuture.completedFuture(
            ReceiveMessageResponse.builder()
                .messages(new ArrayList<>(Arrays.asList(message)))
                .build());
    ReceiveMessageRequest testRequest =
        ReceiveMessageRequest.builder()
            .queueUrl("testUrl")
            .maxNumberOfMessages(1)
            .waitTimeSeconds(AsyncSqsClientConfiguration.DEFAULT_RECEIVE_WAIT_SECONDS)
            .build();
    when(awsAsyncSqsClient.receiveMessage(testRequest)).thenReturn(receiveMessage);
  }

  // dummy queue URL set up
  private void urlSetup(String queueName, String url) {
    GetQueueUrlResponse response = GetQueueUrlResponse.builder().queueUrl(url).build();
    CompletableFuture<GetQueueUrlResponse> queueUrl = CompletableFuture.completedFuture(response);
    GetQueueUrlRequest getUrl = GetQueueUrlRequest.builder().queueName(queueName).build();
    when(awsAsyncSqsClient.getQueueUrl(getUrl)).thenReturn(queueUrl);
  }

  // dummy delete from queue
  private void deleteSetup() {
    DeleteMessageRequest deleteReq =
        DeleteMessageRequest.builder().queueUrl("testUrl").receiptHandle("testHandle").build();
    CompletableFuture<DeleteMessageResponse> deleteResponse =
        CompletableFuture.completedFuture(DeleteMessageResponse.builder().build());
    when(awsAsyncSqsClient.deleteMessage(deleteReq)).thenReturn(deleteResponse);
  }

  // dummy getQueueUrl failure
  private void urlFailureSetup(String queueName) {
    CompletableFuture<GetQueueUrlResponse> queueUrl = new CompletableFuture<>();
    queueUrl.completeExceptionally(new Exception("Fake Exception returned by SQS"));
    GetQueueUrlRequest getUrl = GetQueueUrlRequest.builder().queueName(queueName).build();
    when(awsAsyncSqsClient.getQueueUrl(getUrl)).thenReturn(queueUrl);
  }

  @Test
  public void testUrlRequest() {
    urlSetup("testQueue", "testUrl");
    queueAddSetup();
    ArgumentCaptor<GetQueueUrlRequest> urlReq = ArgumentCaptor.forClass(GetQueueUrlRequest.class);

    // add() will call getQueueUrl
    asyncClient.add("test", "testQueue");

    // testing request
    verify(awsAsyncSqsClient).getQueueUrl(urlReq.capture());
    assertEquals(urlReq.getValue().queueName(), "testQueue");
  }

  @Test
  public void testAdd() {
    urlSetup("testQueue", "testUrl");
    queueAddSetup();
    sentRequest = ArgumentCaptor.forClass(SendMessageRequest.class);
    CompletableFuture<Void> noDelay = asyncClient.add("test", "testQueue");
    verify(awsAsyncSqsClient).sendMessage(sentRequest.capture());

    assertEquals(sentRequest.getValue().messageBody(), "test");
    assertEquals(sentRequest.getValue().queueUrl(), "testUrl");

    verify(awsAsyncSqsClient, times(1)).sendMessage(any(SendMessageRequest.class));

    assertNull(noDelay.join());
  }

  @Test
  public void testAddWithDelay() {
    urlSetup("testQueue", "testUrl");
    queueAddSetup();
    sentRequest = ArgumentCaptor.forClass(SendMessageRequest.class);

    CompletableFuture<Void> delay = asyncClient.add("test", "testQueue", 10);
    verify(awsAsyncSqsClient).sendMessage(sentRequest.capture());

    assertEquals(sentRequest.getValue().messageBody(), "test");
    assertEquals(sentRequest.getValue().queueUrl(), "testUrl");
    assertEquals(sentRequest.getValue().delaySeconds(), new Integer(10));

    verify(awsAsyncSqsClient, times(1)).sendMessage(any(SendMessageRequest.class));

    assertNull(delay.join());
  }

  @Test(expected = Exception.class)
  public void testConsumeInternalsFailure() {
    urlSetup("testQueue", "testUrl");
    receiveSetup();

    // dummy reset visibility
    ChangeMessageVisibilityRequest changeVisibilityReq =
        ChangeMessageVisibilityRequest.builder()
            .queueUrl("testUrl")
            .receiptHandle("testHandle")
            .visibilityTimeout(0)
            .build();
    CompletableFuture<ChangeMessageVisibilityResponse> visibilityResponse =
        CompletableFuture.completedFuture(ChangeMessageVisibilityResponse.builder().build());
    when(awsAsyncSqsClient.changeMessageVisibility(changeVisibilityReq))
        .thenReturn(visibilityResponse);

    ArgumentCaptor<ChangeMessageVisibilityRequest> visibilityReq =
        ArgumentCaptor.forClass(ChangeMessageVisibilityRequest.class);

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new Exception("async consumer completing exceptionally"));
    AsyncConsumer<String> function = (str, threadpool) -> future;

    CompletableFuture<Boolean> result = asyncClient.consume(function, "testQueue");

    // testing setMessageVisible
    verify(awsAsyncSqsClient).changeMessageVisibility(visibilityReq.capture());
    assertEquals(visibilityReq.getValue().queueUrl(), "testUrl");
    assertEquals(visibilityReq.getValue().receiptHandle(), "testHandle");
    assertEquals(visibilityReq.getValue().visibilityTimeout(), new Integer(0));

    verify(awsAsyncSqsClient, times(1))
        .changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));

    // testing final result
    result.join();
  }

  @Test
  public void testConsumeInternalsSuccess() {
    urlSetup("testQueue", "testUrl");
    receiveSetup();
    deleteSetup();
    ArgumentCaptor<ReceiveMessageRequest> messageReq =
        ArgumentCaptor.forClass(ReceiveMessageRequest.class);
    ArgumentCaptor<DeleteMessageRequest> deleteMessageReq =
        ArgumentCaptor.forClass(DeleteMessageRequest.class);
    Consumer<String> function = str -> System.out.println(str);

    CompletableFuture<Boolean> result = asyncClient.consume(function, "testQueue");

    assertTrue(result.join());

    // testing getMessage
    verify(awsAsyncSqsClient).receiveMessage(messageReq.capture());
    assertEquals(messageReq.getValue().queueUrl(), "testUrl");
    assertEquals(messageReq.getValue().maxNumberOfMessages(), new Integer(1));
    assertEquals(
        messageReq.getValue().waitTimeSeconds(),
        new Integer(AsyncSqsClientConfiguration.DEFAULT_RECEIVE_WAIT_SECONDS));

    verify(awsAsyncSqsClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));

    // testing deleteMessage
    verify(awsAsyncSqsClient).deleteMessage(deleteMessageReq.capture());
    assertEquals(deleteMessageReq.getValue().queueUrl(), "testUrl");
    assertEquals(deleteMessageReq.getValue().receiptHandle(), "testHandle");

    verify(awsAsyncSqsClient, times(1)).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  public void testConsumeEmptyQueue() {
    urlSetup("empty", "emptyUrl");

    CompletableFuture<ReceiveMessageResponse> noMessages =
        CompletableFuture.completedFuture(ReceiveMessageResponse.builder().build());
    ReceiveMessageRequest testEmptyRequest =
        ReceiveMessageRequest.builder()
            .queueUrl("emptyUrl")
            .waitTimeSeconds(AsyncSqsClientConfiguration.DEFAULT_RECEIVE_WAIT_SECONDS)
            .maxNumberOfMessages(1)
            .build();
    when(awsAsyncSqsClient.receiveMessage(testEmptyRequest)).thenReturn(noMessages);

    Consumer<String> function = str -> System.out.println(str);
    CompletableFuture<Boolean> response = asyncClient.consume(function, "empty");
    verify(awsAsyncSqsClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));

    assertFalse(response.join());
  }

  @Test
  public void testAsyncConsumeConsumption() {
    urlSetup("testQueue", "testUrl");
    receiveSetup();
    deleteSetup();
    AsyncConsumer<String> function = (str, threadpool) -> CompletableFuture.completedFuture(null);
    CompletableFuture<Boolean> result = asyncClient.consume(function, "testQueue");

    // testing final result
    assertTrue(result.join());
  }

  @Test
  public void testConsumeCompletionWithReceiveFailure() {
    urlSetup("receiveFailure", "receiveFailureUrl");

    CompletableFuture<ReceiveMessageResponse> receiveMessage = new CompletableFuture<>();
    receiveMessage.completeExceptionally(new Exception());
    ReceiveMessageRequest testRequest =
        ReceiveMessageRequest.builder()
            .queueUrl("receiveFailureUrl")
            .maxNumberOfMessages(1)
            .waitTimeSeconds(AsyncSqsClientConfiguration.DEFAULT_RECEIVE_WAIT_SECONDS)
            .build();
    when(awsAsyncSqsClient.receiveMessage(testRequest)).thenReturn(receiveMessage);

    // ensure future is completed when exception in receiveMessage
    Consumer<String> function = System.out::println;
    CompletableFuture<Boolean> result = asyncClient.consume(function, "receiveFailure");

    try {
      result.get(1000, TimeUnit.MICROSECONDS);
    } catch (TimeoutException timeout) {
      fail("Future does not seem to complete when failure in receiveMessage.");
    } catch (Exception ex) {
    }

    urlFailureSetup("receiveFailure");
    // ensure future is completed when exception in get URL within getMessage
    result = asyncClient.consume(function, "receiveFailure");

    try {
      result.get(1000, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeout) {
      fail("Future does not seem to complete when failure in getQueueUrl within receiveMessage.");
    } catch (Exception ex) {
    }
  }

  @Test
  public void testConsumeCompletionWithDeletionFailure() {
    urlSetup("deletionFailure", "deletionFailureUrl");
    receiveSetup();

    DeleteMessageRequest deleteReq =
        DeleteMessageRequest.builder()
            .queueUrl("deletionFailureUrl")
            .receiptHandle("deletionHandle")
            .build();
    CompletableFuture<DeleteMessageResponse> deleteResponse = new CompletableFuture<>();
    deleteResponse.completeExceptionally(new Exception());
    when(awsAsyncSqsClient.deleteMessage(deleteReq)).thenReturn(deleteResponse);

    // ensure future is completed when exception in deleteMessage
    Consumer<String> function = System.out::println;
    CompletableFuture<Boolean> result = asyncClient.consume(function, "deletionFailure");

    try {
      result.get(1000, TimeUnit.MICROSECONDS);
    } catch (TimeoutException timeout) {
      fail("Future does not seem to complete when failure in deleteMessage.");
    } catch (Exception ex) {
    }

    urlFailureSetup("deletionFailure");
    // ensure future is completed when exception in get URL within deleteMessage
    result = asyncClient.consume(function, "deletionFailure");

    try {
      result.get(1000, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeout) {
      fail("Future does not seem to complete when failure in getQueueUrl within deleteMessage.");
    } catch (Exception ex) {
    }
  }

  @Test
  public void testConsumeCompletionWithChangeVisibilityFailure() {
    urlSetup("visibilityFailure", "visibilityFailureUrl");

    ChangeMessageVisibilityRequest changeVisibilityReq =
        ChangeMessageVisibilityRequest.builder()
            .queueUrl("visibilityFailureUrl")
            .receiptHandle("visibilityFailure")
            .visibilityTimeout(0)
            .build();
    CompletableFuture<ChangeMessageVisibilityResponse> visibilityResponse =
        new CompletableFuture<>();
    visibilityResponse.completeExceptionally(new Exception());
    when(awsAsyncSqsClient.changeMessageVisibility(changeVisibilityReq))
        .thenReturn(visibilityResponse);

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new Exception("async consumer completing exceptionally"));
    AsyncConsumer<String> function = (str, threadpool) -> future;

    // ensure future is completed when exception in changeMessageVisibility
    CompletableFuture<Boolean> result = asyncClient.consume(function, "visibilityFailure");

    try {
      result.get(1000, TimeUnit.MICROSECONDS);
    } catch (TimeoutException timeout) {
      fail("Future does not seem to complete when failure in changeMessageVisibility.");
    } catch (Exception ex) {
    }

    urlFailureSetup("visibilityFailure");
    // ensure future is completed when exception in get URL within changeMessageVisibility
    result = asyncClient.consume(function, "visibilityFailure");

    try {
      result.get(1000, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeout) {
      fail(
          "Future does not seem to complete when failure in getQueueUrl within changeMessageVisibility.");
    } catch (Exception ex) {
    }
  }

  @Test
  public void testConsumerConcurrency() {
    asyncClient =
        new AsyncSqsClientImpl(
            awsAsyncSqsClient,
            metrics,
            executor,
            1,
            AsyncSqsClientConfiguration.DEFAULT_RECEIVE_WAIT_SECONDS,
            0, // immediately raise an exception if bulkhead is full
            10);
    urlSetup("testQueue", "testUrl");
    receiveSetup();
    when(awsAsyncSqsClient.deleteMessage(any(DeleteMessageRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(DeleteMessageResponse.builder().build()));

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    List<CompletableFuture<Boolean>> consumeFutures = new ArrayList<>();

    // occupy all slots in the bulkhead
    for (int i = 0; i < 10; i++) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      futures.add(future);
      consumeFutures.add(asyncClient.consume((str, executor) -> future, "testQueue"));
    }

    // now, further consume attempts should cause a wait on the semaphore within the bulkhead
    // however, since we set the max wait ms to 0, calling consume will raise a
    // BulkheadFullException immediately
    BulkheadFullException bulkheadFullException = null;
    try {
      asyncClient.consume((str, executor) -> new CompletableFuture<>(), "testQueue");
    } catch (BulkheadFullException e) {
      bulkheadFullException = e;
    }
    assertNotNull(bulkheadFullException);

    // free a slot in the bulkhead
    futures.get(0).complete(null);

    // now, it should be okay to further consume
    consumeFutures
        .get(0)
        .whenComplete(
            (r, t) ->
                asyncClient.consume((str, executor) -> new CompletableFuture<>(), "testQueue"))
        .join();
  }
}
