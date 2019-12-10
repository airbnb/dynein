/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.dynamodb;

import static java.util.Arrays.asList;

import com.airbnb.dynein.api.JobTokenPayload;
import com.airbnb.dynein.scheduler.Schedule;
import com.airbnb.dynein.scheduler.Schedule.JobStatus;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@UtilityClass
class DynamoDBUtils {
  @AllArgsConstructor
  @Getter
  public enum Attribute {
    SHARD_ID("shard_id", "#shardId"),
    DATE_TOKEN("date_token", "#dateToken"),
    JOB_STATUS("job_status", "#jobStatus"),
    JOB_SPEC("job_spec", "#jobSpec");

    public final String columnName;
    public final String attributeName;
  }

  @AllArgsConstructor
  public enum Value {
    SHARD_ID(":shardId"),
    JOB_STATUS(":jobStatus"),
    DATE_TOKEN(":dateToken"),
    OLD_STATUS(":oldStatus"),
    NEW_STATUS(":newStatus"),
    SCHEDULED_STATUS(":scheduled"),
    ACQUIRED_STATUS(":acquired");
    public final String name;
  }

  @AllArgsConstructor(staticName = "of")
  public class Condition {
    private Attribute attribute;
    private String operator;
    private Value value;

    public String toString() {
      return String.format("%s %s %s", attribute.attributeName, operator, value.name);
    }
  }

  Map<String, AttributeValue> attributeValuesMap(Map<Value, String> input) {
    return input
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                (Entry<Value, String> e) -> e.getKey().name,
                (Entry<Value, String> e) -> AttributeValue.builder().s(e.getValue()).build()));
  }

  @Getter(lazy = true)
  private final Map<String, String> jobStatusAttributeMap = initJobStatusAttributeMap();

  @Getter(lazy = true)
  private final Map<String, String> getOverdueJobsAttributeMap =
      initGetOverdueJobsAttributeNameMap();

  private Map<String, String> initAttributeNameMap(List<Attribute> attributes) {
    return ImmutableMap.copyOf(
        attributes
            .stream()
            .collect(Collectors.toMap(Attribute::getAttributeName, Attribute::getColumnName)));
  }

  private Map<String, String> initJobStatusAttributeMap() {
    return initAttributeNameMap(Collections.singletonList(Attribute.JOB_STATUS));
  }

  private Map<String, String> initGetOverdueJobsAttributeNameMap() {
    return initAttributeNameMap(
        asList(Attribute.SHARD_ID, Attribute.JOB_STATUS, Attribute.DATE_TOKEN));
  }

  Map<String, AttributeValue> getPrimaryKeyFromToken(
      String token, JobTokenPayload payload, int maxShardId) {
    Map<String, AttributeValue> primaryKey = new HashMap<>();
    String date = String.valueOf(payload.getEpochMillis());
    int shard = payload.getLogicalShard();
    primaryKey.put(
        Attribute.SHARD_ID.columnName,
        AttributeValue.builder().s(Integer.toString(shard % maxShardId)).build());
    primaryKey.put(
        Attribute.DATE_TOKEN.columnName,
        AttributeValue.builder().s(String.format("%s#%s", date, token)).build());
    return primaryKey;
  }

  Schedule decodeSchedule(Map<String, AttributeValue> item) {
    return new Schedule(
        item.get(Attribute.DATE_TOKEN.columnName).s(),
        JobStatus.valueOf(item.get(Attribute.JOB_STATUS.columnName).s()),
        item.get(Attribute.JOB_SPEC.columnName).s(),
        item.get(Attribute.SHARD_ID.columnName).s());
  }

  Map<String, AttributeValue> getPrimaryKey(Schedule schedule) {
    Map<String, AttributeValue> item = new HashMap<>();

    item.put(
        Attribute.SHARD_ID.columnName, AttributeValue.builder().s(schedule.getShardId()).build());
    item.put(
        Attribute.DATE_TOKEN.columnName,
        AttributeValue.builder().s(schedule.getDateToken()).build());
    return item;
  }

  Map<String, AttributeValue> toAttributeMap(Schedule schedule) {
    Map<String, AttributeValue> item = new HashMap<>();

    item.put(
        Attribute.SHARD_ID.columnName, AttributeValue.builder().s(schedule.getShardId()).build());
    item.put(
        Attribute.DATE_TOKEN.columnName,
        AttributeValue.builder().s(schedule.getDateToken()).build());
    item.put(
        Attribute.JOB_STATUS.columnName,
        AttributeValue.builder().s(schedule.getStatus().toString()).build());
    item.put(
        Attribute.JOB_SPEC.columnName, AttributeValue.builder().s(schedule.getJobSpec()).build());
    return item;
  }
}
