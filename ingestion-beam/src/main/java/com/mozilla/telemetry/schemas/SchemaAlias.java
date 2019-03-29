/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.schemas;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

@AutoValue
abstract class SchemaAlias {

  @JsonCreator
  static SchemaAlias create(@JsonProperty(value = "base", required = true) String base,
      @JsonProperty(value = "alias", required = true) String alias) {
    return new AutoValue_SchemaAlias(base, alias);
  }

  abstract String base();

  abstract String alias();
}
