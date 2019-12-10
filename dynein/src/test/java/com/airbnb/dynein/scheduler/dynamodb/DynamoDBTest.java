/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.dynamodb;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.airbnb.dynein.api.DyneinJobSpec;
import com.airbnb.dynein.api.InvalidTokenException;
import com.airbnb.dynein.api.JobSchedulePolicy;
import com.airbnb.dynein.api.JobScheduleType;
import com.airbnb.dynein.api.JobTokenPayload;
import com.airbnb.dynein.common.job.JacksonJobSpecTransformer;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.JacksonTokenManager;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.common.utils.TimeUtils;
import com.airbnb.dynein.scheduler.Schedule;
import com.airbnb.dynein.scheduler.Schedule.JobStatus;
import com.airbnb.dynein.scheduler.ScheduleManager;
import com.airbnb.dynein.scheduler.ScheduleManager.SchedulesQueryResponse;
import com.airbnb.dynein.scheduler.config.DynamoDBConfiguration;
import com.airbnb.dynein.scheduler.dynamodb.DynamoDBUtils.Attribute;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import com.airbnb.dynein.scheduler.metrics.NoOpMetricsImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBTest {
  private static final byte[] SERIALIZED_JOB_DATA = {0, 0, 0, 0};
  @Mock private DynamoDbAsyncClient ddbClient;
  private JobSpecTransformer transformer;
  private TokenManager tokenManager;
  private ScheduleManager scheduleManager;
  private Clock clock;
  private String validToken;
  private String tableName;
  private int maxShardId;
  private Metrics metrics;
  private DynamoDBConfiguration ddbConfig;

  @Before
  public void setUp() {
    ddbConfig = new DynamoDBConfiguration();
    maxShardId = 64;
    ObjectMapper mapper = new ObjectMapper();
    transformer = new JacksonJobSpecTransformer(mapper);
    tokenManager = new JacksonTokenManager(mapper);
    tableName = ddbConfig.getSchedulesTableName();
    clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"));
    validToken = tokenManager.generateToken(2, "test-cluster", clock.millis() + 1000);
    metrics = spy(new NoOpMetricsImpl());
    scheduleManager =
        new DynamoDBScheduleManager(
            maxShardId, tokenManager, transformer, clock, metrics, ddbClient, ddbConfig);
  }

  // lifted from original DyneinTest
  private DyneinJobSpec getTestJobSpec(String token, String queueName) {
    JobSchedulePolicy policy =
        JobSchedulePolicy.builder()
            .type(JobScheduleType.SCHEDULED)
            .epochMillis(Instant.now(clock).plusMillis(1000).toEpochMilli())
            .build();

    return DyneinJobSpec.builder()
        .jobToken(token)
        .name("AddJob")
        .queueType("PRODUCTION")
        .queueName(queueName)
        .createAtInMillis(Instant.now().minusMillis(10).toEpochMilli())
        .schedulePolicy(policy)
        .serializedJob(SERIALIZED_JOB_DATA)
        .build();
  }

  private String getToken(int id) {
    return tokenManager.generateToken(id, null, (long) 10);
  }

  private Schedule jobSpecToSchedule(DyneinJobSpec jobSpec) throws InvalidTokenException {
    String date =
        Long.toString(TimeUtils.getInstant(jobSpec.getSchedulePolicy(), clock).toEpochMilli());

    JobTokenPayload token = tokenManager.decodeToken(jobSpec.getJobToken());
    int shard = token.getLogicalShard();
    String message = transformer.serializeJobSpec(jobSpec);

    return new Schedule(
        String.format("%s#%s", date, jobSpec.getJobToken()),
        Schedule.JobStatus.SCHEDULED,
        message,
        Integer.toString(shard % maxShardId));
  }

  public <T> Throwable getException(CompletableFuture<T> future) {
    Throwable t = null;
    try {
      future.get(1000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException ex) {
      t = ex.getCause();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return t;
  }

  @Test
  public void testScheduleJob() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec);
    Map<String, AttributeValue> item = DynamoDBUtils.toAttributeMap(schedule);

    PutItemRequest putItemRequest =
        PutItemRequest.builder().tableName(tableName).item(item).build();
    when(ddbClient.putItem(putItemRequest)).thenReturn(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> response = scheduleManager.addJob(schedule);

    response.get(1000, TimeUnit.MILLISECONDS);
    verify(ddbClient, times(1)).putItem(putItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testScheduleJob_Failure() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test2");
    Schedule schedule = jobSpecToSchedule(jobSpec);
    Map<String, AttributeValue> item = DynamoDBUtils.toAttributeMap(schedule);
    CompletableFuture<PutItemResponse> ret = new CompletableFuture<>();
    Exception putException = new Exception();
    ret.completeExceptionally(putException);

    PutItemRequest putItemRequest =
        PutItemRequest.builder().tableName(tableName).item(item).build();
    when(ddbClient.putItem(putItemRequest)).thenReturn(ret);

    CompletableFuture<Void> response = scheduleManager.addJob(schedule);

    assertSame(getException(response), putException);
    verify(ddbClient, times(1)).putItem(putItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testDeleteJob() throws Exception {
    String token = getToken(1);
    JobTokenPayload tokenPayload = tokenManager.decodeToken(token);
    Map<String, AttributeValue> primaryKey =
        DynamoDBUtils.getPrimaryKeyFromToken(token, tokenPayload, maxShardId);

    Map<String, String> attributeNames = new HashMap<>();
    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeNames.put("#jobStatus", DynamoDBUtils.Attribute.JOB_STATUS.columnName);
    attributeValues.put(
        ":scheduled", AttributeValue.builder().s(Schedule.JobStatus.SCHEDULED.toString()).build());

    DeleteItemRequest deleteItemRequest =
        DeleteItemRequest.builder()
            .tableName(tableName)
            .conditionExpression("#jobStatus = :scheduled")
            .key(primaryKey)
            .expressionAttributeNames(attributeNames)
            .expressionAttributeValues(attributeValues)
            .build();

    when(ddbClient.deleteItem(deleteItemRequest))
        .thenReturn(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> response = scheduleManager.deleteJob(token);
    response.get(1000, TimeUnit.MILLISECONDS);
    verify(ddbClient, times(1)).deleteItem(deleteItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testDeleteJob_Failure() throws Exception {
    String token = getToken(2);
    JobTokenPayload tokenPayload = tokenManager.decodeToken(token);
    Map<String, AttributeValue> primaryKey =
        DynamoDBUtils.getPrimaryKeyFromToken(token, tokenPayload, maxShardId);

    Map<String, String> attributeNames = new HashMap<>();
    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeNames.put("#jobStatus", DynamoDBUtils.Attribute.JOB_STATUS.columnName);
    attributeValues.put(
        ":scheduled", AttributeValue.builder().s(Schedule.JobStatus.SCHEDULED.toString()).build());

    DeleteItemRequest deleteItemRequest =
        DeleteItemRequest.builder()
            .tableName(tableName)
            .conditionExpression("#jobStatus = :scheduled")
            .key(primaryKey)
            .expressionAttributeNames(attributeNames)
            .expressionAttributeValues(attributeValues)
            .build();

    CompletableFuture<DeleteItemResponse> ret = new CompletableFuture<>();
    Exception exception = new Exception();
    ret.completeExceptionally(exception);
    when(ddbClient.deleteItem(deleteItemRequest)).thenReturn(ret);

    CompletableFuture<Void> response = scheduleManager.deleteJob(token);

    assertSame(getException(response), exception);
    verify(ddbClient, times(1)).deleteItem(deleteItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testGetJob() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec);

    JobTokenPayload tokenPayload = tokenManager.decodeToken(validToken);
    Map<String, AttributeValue> primaryKey =
        DynamoDBUtils.getPrimaryKeyFromToken(validToken, tokenPayload, maxShardId);

    GetItemRequest getItemRequest =
        GetItemRequest.builder()
            .key(primaryKey)
            .tableName(tableName)
            .attributesToGet(Collections.singletonList(DynamoDBUtils.Attribute.JOB_SPEC.columnName))
            .build();

    when(ddbClient.getItem(getItemRequest))
        .thenReturn(
            CompletableFuture.completedFuture(
                GetItemResponse.builder()
                    .item(
                        ImmutableMap.of(
                            DynamoDBUtils.Attribute.JOB_SPEC.columnName,
                            AttributeValue.builder().s(schedule.getJobSpec()).build()))
                    .build()));

    CompletableFuture<Schedule> response = scheduleManager.getJob(validToken);

    Assert.assertEquals(response.get(1000, TimeUnit.MILLISECONDS), schedule);

    verify(ddbClient, times(1)).getItem(getItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testGetJob_Failure() throws Throwable {
    String token = getToken(4);
    JobTokenPayload tokenPayload = tokenManager.decodeToken(token);
    Map<String, AttributeValue> primaryKey =
        DynamoDBUtils.getPrimaryKeyFromToken(token, tokenPayload, maxShardId);

    GetItemRequest getItemRequest =
        GetItemRequest.builder()
            .key(primaryKey)
            .tableName(tableName)
            .attributesToGet(Collections.singletonList(DynamoDBUtils.Attribute.JOB_SPEC.columnName))
            .build();

    CompletableFuture<GetItemResponse> ret = new CompletableFuture<>();
    Exception exception = new Exception();
    ret.completeExceptionally(exception);
    when(ddbClient.getItem(getItemRequest)).thenReturn(ret);

    CompletableFuture<Schedule> response = scheduleManager.getJob(token);
    assertSame(getException(response), exception);

    verify(ddbClient, times(1)).getItem(getItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testGetOverdueJobs() throws Exception {
    String partition = "test-partition";
    QueryRequest queryRequest = getQueryRequest(partition, JobStatus.SCHEDULED, Instant.now(clock));
    DyneinJobSpec jobSpec1 = getTestJobSpec(validToken, "test1");
    Schedule schedule1 = jobSpecToSchedule(jobSpec1);
    DyneinJobSpec jobSpec2 = getTestJobSpec(getToken(4), "test2");
    Schedule schedule2 = jobSpecToSchedule(jobSpec2);

    QueryResponse queryResponse =
        QueryResponse.builder()
            .items(
                asList(
                    DynamoDBUtils.toAttributeMap(schedule1),
                    DynamoDBUtils.toAttributeMap(schedule2)))
            .count(2)
            .lastEvaluatedKey(ImmutableMap.of())
            .build();

    when(ddbClient.query(queryRequest))
        .thenReturn(CompletableFuture.completedFuture(queryResponse));

    CompletableFuture<SchedulesQueryResponse> response = scheduleManager.getOverdueJobs(partition);
    Assert.assertEquals(
        response.get(1, TimeUnit.SECONDS),
        SchedulesQueryResponse.of(asList(schedule1, schedule2), false));

    verify(ddbClient, times(1)).query(queryRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testGetOverdueJobs_queryLimit() throws Exception {
    String partition = "test-partition";
    QueryRequest queryRequest = getQueryRequest(partition, JobStatus.SCHEDULED, Instant.now(clock));
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec);

    List<Schedule> queryLimitSchedules = new ArrayList<>();

    for (int i = 0; i < ddbConfig.getQueryLimit(); i++) {
      queryLimitSchedules.add(schedule);
    }

    QueryResponse queryResponse =
        QueryResponse.builder()
            .items(
                queryLimitSchedules
                    .stream()
                    .map(DynamoDBUtils::toAttributeMap)
                    .collect(Collectors.toList()))
            .count(ddbConfig.getQueryLimit())
            .lastEvaluatedKey(ImmutableMap.of())
            .build();

    when(ddbClient.query(queryRequest))
        .thenReturn(CompletableFuture.completedFuture(queryResponse));

    CompletableFuture<SchedulesQueryResponse> response = scheduleManager.getOverdueJobs(partition);
    Assert.assertEquals(
        response.get(1, TimeUnit.SECONDS), SchedulesQueryResponse.of(queryLimitSchedules, true));

    verify(ddbClient, times(1)).query(queryRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testGetOverdueJobs_pagination() throws Exception {
    String partition = "test-partition";
    QueryRequest queryRequest = getQueryRequest(partition, JobStatus.SCHEDULED, Instant.now(clock));
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec);

    List<Schedule> queryLimitSchedules = new ArrayList<>();

    for (int i = 0; i < ddbConfig.getQueryLimit() - 1; i++) {
      queryLimitSchedules.add(schedule);
    }

    QueryResponse queryResponse =
        QueryResponse.builder()
            .items(
                queryLimitSchedules
                    .stream()
                    .map(DynamoDBUtils::toAttributeMap)
                    .collect(Collectors.toList()))
            .count(ddbConfig.getQueryLimit() - 1)
            .lastEvaluatedKey(
                ImmutableMap.of("random", AttributeValue.builder().s("thing").build()))
            .build();

    when(ddbClient.query(queryRequest))
        .thenReturn(CompletableFuture.completedFuture(queryResponse));

    CompletableFuture<SchedulesQueryResponse> response = scheduleManager.getOverdueJobs(partition);
    Assert.assertEquals(
        response.get(1, TimeUnit.SECONDS), SchedulesQueryResponse.of(queryLimitSchedules, true));

    verify(ddbClient, times(1)).query(queryRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testGetOverdueJobs_failure() {
    String partition = "test-partition";
    QueryRequest queryRequest = getQueryRequest(partition, JobStatus.SCHEDULED, Instant.now(clock));
    Exception exception = new Exception();

    CompletableFuture<QueryResponse> fut = new CompletableFuture<>();
    fut.completeExceptionally(exception);

    when(ddbClient.query(queryRequest)).thenReturn(fut);

    CompletableFuture<SchedulesQueryResponse> response = scheduleManager.getOverdueJobs(partition);
    assertSame(getException(response), exception);

    verify(ddbClient, times(1)).query(queryRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testUpdateStatus() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec);
    UpdateItemRequest updateItemRequest =
        getUpdateItemReq(schedule, JobStatus.SCHEDULED, JobStatus.ACQUIRED);

    when(ddbClient.updateItem(updateItemRequest))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpdateItemResponse.builder()
                    .attributes(
                        ImmutableMap.of(
                            Attribute.JOB_STATUS.columnName,
                            AttributeValue.builder().s(JobStatus.ACQUIRED.name()).build()))
                    .build()));

    CompletableFuture<Schedule> response =
        scheduleManager.updateStatus(schedule, JobStatus.SCHEDULED, JobStatus.ACQUIRED);
    Assert.assertEquals(response.get(1, TimeUnit.SECONDS), schedule.withStatus(JobStatus.ACQUIRED));

    verify(ddbClient, times(1)).updateItem(updateItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testUpdateStatus_emptyResponse() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec);
    UpdateItemRequest updateItemRequest =
        getUpdateItemReq(schedule, JobStatus.SCHEDULED, JobStatus.ACQUIRED);

    when(ddbClient.updateItem(updateItemRequest))
        .thenReturn(CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));

    CompletableFuture<Schedule> response =
        scheduleManager.updateStatus(schedule, JobStatus.SCHEDULED, JobStatus.ACQUIRED);

    Throwable exception = getException(response);
    assertTrue(exception instanceof IllegalStateException);
    assertEquals(exception.getMessage(), "Status update successful but status isn't returned.");

    verify(ddbClient, times(1)).updateItem(updateItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testUpdateStatus_unknownResponse() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec);
    UpdateItemRequest updateItemRequest =
        getUpdateItemReq(schedule, JobStatus.SCHEDULED, JobStatus.ACQUIRED);

    when(ddbClient.updateItem(updateItemRequest))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpdateItemResponse.builder()
                    .attributes(
                        ImmutableMap.of(
                            Attribute.JOB_STATUS.columnName,
                            AttributeValue.builder().s("magic").build()))
                    .build()));

    CompletableFuture<Schedule> response =
        scheduleManager.updateStatus(schedule, JobStatus.SCHEDULED, JobStatus.ACQUIRED);

    Throwable exception = getException(response);
    assertTrue(exception instanceof IllegalArgumentException);
    assertEquals(
        exception.getMessage(),
        "No enum constant com.airbnb.dynein.scheduler.Schedule.JobStatus.magic");

    verify(ddbClient, times(1)).updateItem(updateItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testUpdateStatus_failure() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec);
    UpdateItemRequest updateItemRequest =
        getUpdateItemReq(schedule, JobStatus.SCHEDULED, JobStatus.ACQUIRED);

    Exception exception = new Exception();
    CompletableFuture<UpdateItemResponse> response = new CompletableFuture<>();
    response.completeExceptionally(exception);
    when(ddbClient.updateItem(updateItemRequest)).thenReturn(response);

    CompletableFuture<Schedule> ret =
        scheduleManager.updateStatus(schedule, JobStatus.SCHEDULED, JobStatus.ACQUIRED);

    assertSame(getException(ret), exception);

    verify(ddbClient, times(1)).updateItem(updateItemRequest);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testDeleteDispatchedJob() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec).withStatus(JobStatus.ACQUIRED);
    DeleteItemRequest request = getDeleteItemRequest(schedule);

    when(ddbClient.deleteItem(request))
        .thenReturn(CompletableFuture.completedFuture(DeleteItemResponse.builder().build()));

    CompletableFuture<Void> ret = scheduleManager.deleteDispatchedJob(schedule);
    assertNull(ret.get(1, TimeUnit.SECONDS));

    verify(ddbClient, times(1)).deleteItem(request);
    verifyNoMoreInteractions(ddbClient);
  }

  @Test
  public void testDeleteDispatchedJob_failure() throws Exception {
    DyneinJobSpec jobSpec = getTestJobSpec(validToken, "test1");
    Schedule schedule = jobSpecToSchedule(jobSpec).withStatus(JobStatus.ACQUIRED);
    DeleteItemRequest request = getDeleteItemRequest(schedule);

    Exception exception = new Exception();
    CompletableFuture<DeleteItemResponse> response = new CompletableFuture<>();
    response.completeExceptionally(exception);
    when(ddbClient.deleteItem(request)).thenReturn(response);

    CompletableFuture<Void> ret = scheduleManager.deleteDispatchedJob(schedule);
    assertSame(getException(ret), exception);

    verify(ddbClient, times(1)).deleteItem(request);
    verifyNoMoreInteractions(ddbClient);
  }

  private UpdateItemRequest getUpdateItemReq(
      Schedule schedule, Schedule.JobStatus oldStatus, Schedule.JobStatus newStatus) {
    Map<String, AttributeValue> primaryKey = DynamoDBUtils.getPrimaryKey(schedule);
    Map<String, String> attributeNames = new HashMap<>();
    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeNames.put("#jobStatus", DynamoDBUtils.Attribute.JOB_STATUS.columnName);
    attributeValues.put(":oldStatus", AttributeValue.builder().s(oldStatus.name()).build());
    attributeValues.put(":newStatus", AttributeValue.builder().s(newStatus.name()).build());

    String updated = "SET #jobStatus = :newStatus";

    return UpdateItemRequest.builder()
        .tableName(this.tableName)
        .key(primaryKey)
        .conditionExpression("#jobStatus = :oldStatus")
        .expressionAttributeNames(attributeNames)
        .expressionAttributeValues(attributeValues)
        .updateExpression(updated)
        .returnValues(ReturnValue.UPDATED_NEW)
        .build();
  }

  private DeleteItemRequest getDeleteItemRequest(Schedule schedule) {
    Map<String, AttributeValue> primaryKey = DynamoDBUtils.getPrimaryKey(schedule);
    Map<String, String> attributeNames = new HashMap<>();
    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeNames.put("#jobStatus", DynamoDBUtils.Attribute.JOB_STATUS.columnName);
    attributeValues.put(
        ":acquired", AttributeValue.builder().s(Schedule.JobStatus.ACQUIRED.toString()).build());

    return DeleteItemRequest.builder()
        .tableName(tableName)
        .conditionExpression("#jobStatus = :acquired")
        .key(primaryKey)
        .expressionAttributeNames(attributeNames)
        .expressionAttributeValues(attributeValues)
        .build();
  }

  private QueryRequest getQueryRequest(String partition, JobStatus jobStatus, Instant instant) {
    Map<String, AttributeValue> values = new HashMap<>();
    Map<String, String> names = new HashMap<>();

    String keyCondition = "#shardId = :shardId and #dateToken < :dateToken";
    String filter = "#jobStatus = :jobStatus";
    String now = Long.toString(instant.toEpochMilli());
    values.put(":shardId", AttributeValue.builder().s(partition).build());
    values.put(":dateToken", AttributeValue.builder().s(now).build());
    values.put(":jobStatus", AttributeValue.builder().s(jobStatus.toString()).build());
    names.put("#shardId", DynamoDBUtils.Attribute.SHARD_ID.columnName);
    names.put("#dateToken", DynamoDBUtils.Attribute.DATE_TOKEN.columnName);
    names.put("#jobStatus", DynamoDBUtils.Attribute.JOB_STATUS.columnName);

    return QueryRequest.builder()
        .tableName(tableName)
        .keyConditionExpression(keyCondition)
        .filterExpression(filter)
        .expressionAttributeValues(values)
        .expressionAttributeNames(names)
        .limit(ddbConfig.getQueryLimit())
        .build();
  }
}
