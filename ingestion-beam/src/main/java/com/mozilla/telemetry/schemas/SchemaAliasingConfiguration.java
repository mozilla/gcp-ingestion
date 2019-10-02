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
