/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.common.exceptions;

import lombok.NonNull;

abstract class DyneinClientException extends RuntimeException {
  private static final long serialVersionUID = 5899411497145720750L;

  DyneinClientException(@NonNull final String message, @NonNull final Throwable cause) {
    super(message, cause);
  }
}
