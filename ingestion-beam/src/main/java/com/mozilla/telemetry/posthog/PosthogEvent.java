package com.mozilla.telemetry.posthog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.values.TypeDescriptor;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class PosthogEvent implements Serializable {

  public abstract String getUserId();

  abstract String getEventType();

  public abstract long getTime();

  public abstract String getPlatform();

  public abstract Integer getSampleId();

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

  @Nullable
  abstract String getDeviceModel();

  @Nullable
  abstract String getDeviceManufacturer();

  @Nullable
  abstract String getLanguage();

  @Nullable
  abstract Map<String, String> getExperiments();

  abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_PosthogEvent.Builder();
  }

  /**
   * Convert event to JSON.
   */
  public ObjectNode toJson() {
    ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode json = objectMapper.createObjectNode();
    json.put("event", getEventType());

    final ObjectNode properties = objectMapper.createObjectNode();
    properties.put("distinct_id", getUserId());

    // Add all other fields as properties except userId and eventType
    if (getSampleId() != null) {
      properties.put("sample_id", getSampleId());
    }
    if (getAppVersion() != null && !getAppVersion().equals("null")) {
      properties.put("app_version", getAppVersion());
    }
    if (getEventExtras() != null && !getEventExtras().equals("null")) {
      try {
        JsonNode extras = objectMapper.readTree(getEventExtras());
        properties.set("event_extras", extras);
      } catch (Exception e) {
        // ignore parse error
      }
    }
    if (getOsName() != null && !getOsName().equals("null")) {
      properties.put("os_name", getOsName());
    }
    if (getOsVersion() != null && !getOsVersion().equals("null")) {
      properties.put("os_version", getOsVersion());
    }
    if (getCountry() != null && !getCountry().equals("null")) {
      properties.put("country", getCountry());
    }
    if (getDeviceModel() != null && !getDeviceModel().equals("null")) {
      properties.put("device_model", getDeviceModel());
    }
    if (getDeviceManufacturer() != null && !getDeviceManufacturer().equals("null")) {
      properties.put("device_manufacturer", getDeviceManufacturer());
    }
    if (getPlatform() != null && !getPlatform().equals("null")) {
      properties.put("platform", getPlatform());
    }
    if (getLanguage() != null && !getLanguage().equals("null")) {
      properties.put("language", getLanguage());
    }
    if (getExperiments() != null) {
      properties.set("experiments", objectMapper.valueToTree(getExperiments()));
    }

    json.set("properties", properties);

    return json;
  }

  /**
   * Get a Coder for a PosthogEvent.
   */
  public static Coder<PosthogEvent> getCoder() {
    AutoValueSchema autoValueSchema = new AutoValueSchema();
    TypeDescriptor<PosthogEvent> td = TypeDescriptor.of(PosthogEvent.class);
    Schema schema = autoValueSchema.schemaFor(td);
    if (schema != null) {
      return SchemaCoder.of(schema, td, autoValueSchema.toRowFunction(td),
          autoValueSchema.fromRowFunction(td));
    }
    return null;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setUserId(String value);

    public abstract Builder setEventType(String value);

    public abstract Builder setTime(long value);

    public abstract Builder setPlatform(String value);

    public abstract Builder setSampleId(Integer value);

    public abstract Builder setAppVersion(String value);

    public abstract Builder setEventExtras(String value);

    public abstract Builder setOsName(String value);

    public abstract Builder setOsVersion(String value);

    public abstract Builder setCountry(String value);

    public abstract Builder setDeviceModel(String value);

    public abstract Builder setDeviceManufacturer(String value);

    public abstract Builder setLanguage(String value);

    public abstract Builder setExperiments(Map<String, String> value);

    public abstract PosthogEvent build();
  }
}
