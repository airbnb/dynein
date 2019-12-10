/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.common.token;

import com.airbnb.dynein.api.InvalidTokenException;
import com.airbnb.dynein.api.JobTokenPayload;
import com.airbnb.dynein.common.utils.UuidUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.util.UUID;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor = @__(@Inject))
public class JacksonTokenManager implements TokenManager {
  private ObjectMapper objectMapper;

  @Override
  public void validateToken(String token) throws InvalidTokenException {
    decodeToken(token);
  }

  @Override
  public JobTokenPayload decodeToken(String token) throws InvalidTokenException {
    try {
      return objectMapper.readValue(BaseEncoding.base64Url().decode(token), JobTokenPayload.class);
    } catch (IOException e) {
      throw new InvalidTokenException(e);
    }
  }

  @Override
  public String generateToken(long associatedId, String logicalCluster, Long epochMillis) {
    UUID uuid = UUID.randomUUID();
    JobTokenPayload payload =
        JobTokenPayload.builder()
            .logicalShard((short) Math.floorMod(associatedId, MAX_LOGICAL_SHARD))
            .logicalCluster(logicalCluster)
            .epochMillis(epochMillis)
            .uuid(UuidUtils.byteArrayFromUuid(uuid))
            .build();
    try {
      return BaseEncoding.base64Url().encode(objectMapper.writeValueAsBytes(payload));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
