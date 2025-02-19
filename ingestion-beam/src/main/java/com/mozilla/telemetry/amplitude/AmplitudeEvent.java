package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.value.AutoValue;
import com.mozilla.telemetry.util.Json;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.values.TypeDescriptor;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class AmplitudeEvent implements Serializable {

  abstract String getUserId();

  abstract String getEventType();

  public abstract long getTime();

  public abstract String getPlatform();

  @Nullable
  abstract String getAppVersion();

  @Nullable
  abstract String getEventExtras();

  @Nullable
  abstract String getOsName();

  @Nullable
  abstract String getOsVersion();

  @Nullable
  abstract String getCountry();

  abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_AmplitudeEvent.Builder();
  }

  /** 
   * Convert amplitude event to JSON to send in HTTP request.
  */
  public ObjectNode toJson() throws JsonProcessingException {
    final ObjectNode json = Json.createObjectNode();
    json.put("user_id", getUserId());

    final ObjectNode eventExtras = Json.createObjectNode();
    ObjectMapper objectMapper = new ObjectMapper();
    if (getEventExtras() == null || getEventExtras().equals("null")) {
      eventExtras.put("extras", (ObjectNode) new ObjectMapper().readTree("{}"));
    } else {
      eventExtras.put("extras", (ObjectNode) new ObjectMapper().readTree(getEventExtras()));
    }

    json.put("event_properties", eventExtras);
    json.put("event_type", getEventType());
    json.put("platform", getPlatform());

    if (getAppVersion() != null && !getAppVersion().equals("null")) {
      json.put("app_version", getAppVersion());
    }

    if (getOsName() != null && !getOsName().equals("null")) {
      json.put("os_name", getOsName());
    }

    if (getOsVersion() != null && !getOsVersion().equals("null")) {
      json.put("os_version", getOsVersion());
    }

    if (getCountry() != null && !getCountry().equals("null")) {
      json.put("country", getCountry());
    }

    return json;
  }

  /**
   * There are a few places that we can not use an inferred Coder. This provides a
   * SchemaCoder for the AmplitudeEvent class. Returns `null` if a schema cannot be found.
   *
   * @return The schema coder for a AmplitudeEvent.
   */
  public static Coder<AmplitudeEvent> getCoder() {
    AutoValueSchema autoValueSchema = new AutoValueSchema();
    TypeDescriptor<AmplitudeEvent> td = TypeDescriptor.of(AmplitudeEvent.class);
    Schema schema = autoValueSchema.schemaFor(td);
    if (schema != null) {
      return SchemaCoder.of(schema, td, autoValueSchema.toRowFunction(td),
          autoValueSchema.fromRowFunction(td));
    }
    return null;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setUserId(String newUserId);

    public abstract Builder setEventType(String newEventType);

    public abstract Builder setTime(long newTime);

    public abstract Builder setPlatform(String newPlatform);

    public abstract Builder setOsName(String newOsName);

    public abstract Builder setOsVersion(String newVersion);

    public abstract Builder setCountry(String newCountry);

    public abstract Builder setEventExtras(String newEventExtras);

    public abstract Builder setAppVersion(String newAppVersion);

    public abstract AmplitudeEvent build();
  }
}
