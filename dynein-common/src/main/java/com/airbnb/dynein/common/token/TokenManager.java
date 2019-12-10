/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.common.token;

import com.airbnb.dynein.api.InvalidTokenException;
import com.airbnb.dynein.api.JobTokenPayload;

public interface TokenManager {
  short MAX_LOGICAL_SHARD = 1024;

  void validateToken(final String token) throws InvalidTokenException;

  JobTokenPayload decodeToken(String token) throws InvalidTokenException;

  String generateToken(final long associatedId, String logicalCluster, Long epochMillis);
}
