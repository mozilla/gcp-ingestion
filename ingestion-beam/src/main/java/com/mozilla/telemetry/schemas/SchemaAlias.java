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
