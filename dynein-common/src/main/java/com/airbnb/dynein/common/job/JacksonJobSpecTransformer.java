/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.common.job;

import com.airbnb.dynein.api.DyneinJobSpec;
import com.airbnb.dynein.common.exceptions.JobDeserializationException;
import com.airbnb.dynein.common.exceptions.JobSerializationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class JacksonJobSpecTransformer implements JobSpecTransformer {

  private final ObjectMapper objectMapper;

  public String serializeJobSpec(final DyneinJobSpec jobSpec) {
    try {
      log.debug("Serializing DyneinJobSpec {}.", jobSpec);

      return objectMapper.writeValueAsString(jobSpec);
    } catch (Exception ex) {
      log.error("Failed to serializeJobData job spec");

      throw new JobSerializationException("Failed to serializeJobData job spec.", ex);
    }
  }

  public DyneinJobSpec deserializeJobSpec(final String data) {
    try {
      log.debug("Deserializing job data.");

      return objectMapper.readValue(data, DyneinJobSpec.class);
    } catch (Exception ex) {
      log.error("Failed to deserializeJobData job data.", ex);

      throw new JobDeserializationException("Failed to deserializeJobData job data.", ex);
    }
  }
}
