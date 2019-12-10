/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.common.job;

import com.airbnb.dynein.api.DyneinJobSpec;

public interface JobSpecTransformer {
  String serializeJobSpec(final DyneinJobSpec jobSpec);

  DyneinJobSpec deserializeJobSpec(final String data);
}
