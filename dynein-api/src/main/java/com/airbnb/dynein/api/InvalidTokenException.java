/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.api;

public class InvalidTokenException extends Exception {
  public InvalidTokenException(Throwable cause) {
    super(cause);
  }
}
