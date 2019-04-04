/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.schemas;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import java.util.List;

@AutoValue
abstract class SchemaAliasingConfiguration {

  @JsonCreator
  static SchemaAliasingConfiguration create(
      @JsonProperty(value = "aliases", required = true) List<SchemaAlias> aliases) {
    return new AutoValue_SchemaAliasingConfiguration(aliases);
  }

  abstract List<SchemaAlias> aliases();
}
