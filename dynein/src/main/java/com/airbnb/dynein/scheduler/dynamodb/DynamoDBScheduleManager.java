/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.dynamodb;

import com.airbnb.dynein.api.DyneinJobSpec;
import com.airbnb.dynein.api.InvalidTokenException;
import com.airbnb.dynein.api.JobTokenPayload;
import com.airbnb.dynein.common.job.JobSpecTransformer;
import com.airbnb.dynein.common.token.TokenManager;
import com.airbnb.dynein.scheduler.Schedule;
import com.airbnb.dynein.scheduler.Schedule.JobStatus;
import com.airbnb.dynein.scheduler.ScheduleManager;
import com.airbnb.dynein.scheduler.config.DynamoDBConfiguration;
import com.airbnb.dynein.scheduler.dynamodb.DynamoDBUtils.Attribute;
import com.airbnb.dynein.scheduler.dynamodb.DynamoDBUtils.Condition;
import com.airbnb.dynein.scheduler.dynamodb.DynamoDBUtils.Value;
import com.airbnb.dynein.scheduler.metrics.Metrics;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import io.reactivex.Flowable;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@Slf4j
public class DynamoDBScheduleManager extends ScheduleManager {
  private final DynamoDbAsyncClient ddbClient;
  private final DynamoDBConfiguration ddbConfig;

  DynamoDBScheduleManager(
      int maxShardId,
      TokenManager tokenManager,
      JobSpecTransformer jobSpecTransformer,
      Clock clock,
      Metrics metrics,
      DynamoDbAsyncClient ddbClient,
      DynamoDBConfiguration ddbConfig) {
    super(maxShardId, tokenManager, jobSpecTransformer, clock, metrics);
    this.ddbClient = ddbClient;
    this.ddbConfig = ddbConfig;
  }

  @Override
  public CompletableFuture<Void> addJob(Schedule schedule) {
    CompletableFuture<Void> ret = new CompletableFuture<>();
    Stopwatch stopwatch = Stopwatch.createStarted();
    DyneinJobSpec jobSpec = jobSpecTransformer.deserializeJobSpec(schedule.getJobSpec());
    try {
      Map<String, AttributeValue> item = DynamoDBUtils.toAttributeMap(schedule);
      PutItemRequest putItemRequest =
          PutItemRequest.builder().tableName(ddbConfig.getSchedulesTableName()).item(item).build();

      ddbClient
          .putItem(putItemRequest)
          .whenComplete(
              (it, ex) -> {
                if (ex != null) {
                  stopwatch.stop();
                  log.error("Error scheduling job {}", jobSpec.getJobToken(), ex);
                  metrics.storeJobError(ex, jobSpec.getQueueName());
                  ret.completeExceptionally(ex);
                } else {
                  stopwatch.stop();
                  log.info("Scheduled job {}", jobSpec.getJobToken());
                  long time = stopwatch.elapsed(TimeUnit.NANOSECONDS);
                  metrics.storeJob(time, jobSpec, schedule.getShardId());
                  ret.complete(null);
                }
              });

    } catch (Exception ex) {
      log.error("Error scheduling job {}", jobSpec.getJobToken(), ex);
      metrics.storeJobError(ex, jobSpec.getQueueName());
      ret.completeExceptionally(ex);
    }
    return ret;
  }

  private <T> CompletableFuture<Void> asyncForeach(
      Flowable<T> f, Function<T, CompletableFuture<Void>> function) {
    CompletableFuture<List<T>> fut = new CompletableFuture<>();
    f.toList().doOnError(fut::completeExceptionally).subscribe(fut::complete);
    return fut.thenCompose(
        items ->
            CompletableFuture.allOf(
                items.stream().map(function).toArray(CompletableFuture[]::new)));
  }

  @Override
  public CompletableFuture<Void> recoverStuckJobs(String partition, Instant instant) {
    AtomicInteger totalCount = new AtomicInteger(0);
    AtomicInteger successCount = new AtomicInteger(0);
    log.info("Starting recoverStuckJobs for partition: {}, with lookAhead: {}", partition, instant);
    return asyncForeach(
            Flowable.fromPublisher(
                    ddbClient.queryPaginator(
                        makeQueryRequestForOverdueJobs(partition, instant, JobStatus.ACQUIRED)))
                .flatMap(queryResponse -> Flowable.fromIterable(queryResponse.items()))
                .map(DynamoDBUtils::decodeSchedule),
            schedule -> {
              log.info(
                  "Recovering job {} in partition {} from ACQUIRED to SCHEDULED",
                  schedule.getDateToken(),
                  schedule.getShardId());
              return updateStatus(schedule, JobStatus.ACQUIRED, JobStatus.SCHEDULED)
                  .handle(
                      (result, ex) -> {
                        totalCount.incrementAndGet();
                        if (ex == null && result.getStatus().equals(JobStatus.SCHEDULED)) {
                          successCount.incrementAndGet();
                          log.info(
                              "Successfully recovered job {} in partition {} from ACQUIRED to SCHEDULED",
                              schedule.getDateToken(),
                              schedule.getShardId());
                        } else {
                          log.warn(
                              "Failed to recover job {} in partition {} from ACQUIRED to SCHEDULED",
                              schedule.getDateToken(),
                              schedule.getShardId());
                        }
                        return null;
                      });
            })
        .whenComplete(
            (result, ex) -> {
              log.info(
                  "Recovered {} of {} jobs from ACQUIRED state back to SCHEDULED state in partition {}.",
                  successCount.get(),
                  totalCount.get(),
                  partition);
              metrics.recoverStuckJob(partition, successCount.get(), totalCount.get());
            });
  }

  @Override
  public CompletableFuture<Schedule> getJob(String token) {
    try {
      JobTokenPayload tokenPayload = tokenManager.decodeToken(token);
      Map<String, AttributeValue> primaryKey =
          DynamoDBUtils.getPrimaryKeyFromToken(token, tokenPayload, maxShardId);

      GetItemRequest getItemRequest =
          GetItemRequest.builder()
              .key(primaryKey)
              .tableName(ddbConfig.getSchedulesTableName())
              .attributesToGet(Collections.singletonList(Attribute.JOB_SPEC.columnName))
              .build();

      return ddbClient
          .getItem(getItemRequest)
          .thenApply(
              item -> {
                try {
                  return makeSchedule(item.item().get(Attribute.JOB_SPEC.columnName).s());
                } catch (InvalidTokenException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (InvalidTokenException ex) {
      CompletableFuture<Schedule> future = new CompletableFuture<>();
      future.completeExceptionally(ex);
      return future;
    }
  }

  @Override
  public CompletableFuture<Void> deleteJob(String token) {
    try {
      JobTokenPayload tokenPayload = tokenManager.decodeToken(token);
      Map<String, AttributeValue> primaryKey =
          DynamoDBUtils.getPrimaryKeyFromToken(token, tokenPayload, maxShardId);

      Map<String, AttributeValue> attributeValues =
          DynamoDBUtils.attributeValuesMap(
              ImmutableMap.of(Value.SCHEDULED_STATUS, Schedule.JobStatus.SCHEDULED.toString()));

      DeleteItemRequest deleteItemRequest =
          DeleteItemRequest.builder()
              .tableName(ddbConfig.getSchedulesTableName())
              .conditionExpression(
                  Condition.of(Attribute.JOB_STATUS, "=", Value.SCHEDULED_STATUS).toString())
              .key(primaryKey)
              .expressionAttributeNames(DynamoDBUtils.getJobStatusAttributeMap())
              .expressionAttributeValues(attributeValues)
              .build();

      return ddbClient
          .deleteItem(deleteItemRequest)
          .whenComplete(
              (response, ex) -> {
                if (ex != null) {
                  log.error(
                      "Error deleting job {} from table {}",
                      token,
                      ddbConfig.getSchedulesTableName(),
                      ex);
                } else {
                  log.info(
                      "Deleted job {} from table {}", token, ddbConfig.getSchedulesTableName());
                }
              })
          .thenApply(response -> null);
    } catch (InvalidTokenException ex) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(ex);
      return future;
    }
  }

  private QueryRequest makeQueryRequestForOverdueJobs(
      String partition, Instant instant, JobStatus jobStatus) {
    String keyCondition =
        Condition.of(Attribute.SHARD_ID, "=", Value.SHARD_ID)
            + " and "
            + Condition.of(Attribute.DATE_TOKEN, "<", Value.DATE_TOKEN);
    String filter = Condition.of(Attribute.JOB_STATUS, "=", Value.JOB_STATUS).toString();

    String now = Long.toString(instant.toEpochMilli());
    Map<String, AttributeValue> attributeValues =
        DynamoDBUtils.attributeValuesMap(
            ImmutableMap.of(
                Value.SHARD_ID, partition,
                Value.DATE_TOKEN, now,
                Value.JOB_STATUS, jobStatus.toString()));
    return QueryRequest.builder()
        .tableName(ddbConfig.getSchedulesTableName())
        .keyConditionExpression(keyCondition)
        .filterExpression(filter)
        .expressionAttributeValues(attributeValues)
        .expressionAttributeNames(DynamoDBUtils.getGetOverdueJobsAttributeMap())
        .limit(ddbConfig.getQueryLimit())
        .build();
  }

  @Override
  public CompletableFuture<SchedulesQueryResponse> getOverdueJobs(
      @NonNull String partition, Instant instant) {
    return ddbClient
        .query(makeQueryRequestForOverdueJobs(partition, instant, JobStatus.SCHEDULED))
        .whenComplete(
            (res, ex) -> {
              if (ex == null) {
                log.info(
                    "Query for overdue jobs in partition {} at time {} successful",
                    partition,
                    instant.toEpochMilli());
                metrics.queryOverdue(partition);
              } else {
                log.error(
                    "Query for overdue jobs in partition {} at time {} failed",
                    partition,
                    instant.toEpochMilli(),
                    ex);
                metrics.queryOverdueError(ex, partition);
              }
            })
        .thenApply(
            queryResponse ->
                SchedulesQueryResponse.of(
                    queryResponse
                        .items()
                        .stream()
                        .map(DynamoDBUtils::decodeSchedule)
                        .collect(Collectors.toList()),
                    queryResponse.count() == ddbConfig.getQueryLimit()
                        || !queryResponse.lastEvaluatedKey().isEmpty()));
  }

  @Override
  public CompletableFuture<Schedule> updateStatus(
      Schedule schedule, JobStatus oldStatus, JobStatus newStatus) {
    Map<String, AttributeValue> primaryKey = DynamoDBUtils.getPrimaryKey(schedule);
    Map<String, AttributeValue> attributeValues =
        DynamoDBUtils.attributeValuesMap(
            ImmutableMap.of(
                Value.OLD_STATUS, oldStatus.toString(),
                Value.NEW_STATUS, newStatus.toString()));

    String updated = "SET " + Condition.of(Attribute.JOB_STATUS, "=", Value.NEW_STATUS);

    UpdateItemRequest updateItemRequest =
        UpdateItemRequest.builder()
            .tableName(ddbConfig.getSchedulesTableName())
            .key(primaryKey)
            .conditionExpression(
                Condition.of(Attribute.JOB_STATUS, "=", Value.OLD_STATUS).toString())
            .expressionAttributeNames(DynamoDBUtils.getJobStatusAttributeMap())
            .expressionAttributeValues(attributeValues)
            .updateExpression(updated)
            .returnValues(ReturnValue.UPDATED_NEW)
            .build();

    return ddbClient
        .updateItem(updateItemRequest)
        .whenComplete(
            (response, exception) -> {
              DyneinJobSpec jobSpec = jobSpecTransformer.deserializeJobSpec(schedule.getJobSpec());
              if (exception != null) {
                log.error(
                    "Failed to set job {} to {}",
                    jobSpec.getJobToken(),
                    newStatus.toString(),
                    exception);
                metrics.updateJobStatusError(exception, oldStatus.toString(), newStatus.toString());
              } else {
                log.info("Set job {} to {}", jobSpec.getJobToken(), newStatus.toString());
                metrics.updateJobStatus(oldStatus.toString(), newStatus.toString());
              }
            })
        .thenApply(
            response -> {
              JobStatus updatedStatus =
                  Optional.ofNullable(response.attributes().get(Attribute.JOB_STATUS.columnName))
                      .map(attr -> JobStatus.valueOf(attr.s()))
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "Status update successful but status isn't returned."));
              return schedule.withStatus(updatedStatus);
            });
  }

  @Override
  public CompletableFuture<Void> deleteDispatchedJob(Schedule schedule) {
    Map<String, AttributeValue> primaryKey = DynamoDBUtils.getPrimaryKey(schedule);
    Map<String, AttributeValue> attributeValues =
        DynamoDBUtils.attributeValuesMap(
            ImmutableMap.of(Value.ACQUIRED_STATUS, Schedule.JobStatus.ACQUIRED.toString()));
    DeleteItemRequest deleteItemRequest =
        DeleteItemRequest.builder()
            .tableName(ddbConfig.getSchedulesTableName())
            .conditionExpression(
                Condition.of(Attribute.JOB_STATUS, "=", Value.ACQUIRED_STATUS).toString())
            .key(primaryKey)
            .expressionAttributeNames(DynamoDBUtils.getJobStatusAttributeMap())
            .expressionAttributeValues(attributeValues)
            .build();

    return ddbClient
        .deleteItem(deleteItemRequest)
        .whenComplete(
            (response, exception) -> {
              DyneinJobSpec jobSpec = jobSpecTransformer.deserializeJobSpec(schedule.getJobSpec());
              if (exception != null) {
                log.error(
                    "Error deleting job {} from table {}",
                    jobSpec.getJobToken(),
                    ddbConfig.getSchedulesTableName(),
                    exception);
                metrics.deleteDispatchedJobError(exception, jobSpec.getQueueName());
              } else {
                log.info(
                    "Deleted job {} from table {}",
                    jobSpec.getJobToken(),
                    ddbConfig.getSchedulesTableName());
                metrics.deleteDispatchedJob(jobSpec.getQueueName());
              }
            })
        .thenApply(response -> null);
  }

  @Override
  public void close() {
    ddbClient.close();
  }
}
