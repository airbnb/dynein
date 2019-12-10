/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.common.exceptions;

import lombok.NonNull;

public final class JobDeserializationException extends DyneinClientException {
  private static final long serialVersionUID = 6333070716140128729L;

  public JobDeserializationException(
      @NonNull final String message, @NonNull final Throwable cause) {
    super(message, cause);
  }
}
