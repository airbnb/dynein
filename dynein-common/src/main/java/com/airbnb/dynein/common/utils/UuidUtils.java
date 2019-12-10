/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.common.utils;

import java.nio.ByteBuffer;
import java.util.UUID;
import lombok.experimental.UtilityClass;

@UtilityClass
public class UuidUtils {
  public byte[] byteArrayFromUuid(UUID uuid) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
    return buffer.array();
  }

  public UUID uuidFromByteArray(byte[] byteArray) {
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    return new UUID(buffer.getLong(), buffer.getLong());
  }
}
