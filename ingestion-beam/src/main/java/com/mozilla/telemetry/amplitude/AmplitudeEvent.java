package com.mozilla.telemetry.amplitude;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.json.JSONObject;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class AmplitudeEvent implements Serializable {

  abstract String getUserId();

  abstract String getEventType();

  public abstract long getTime();

  @Nullable
  abstract String getAppVersion();

  @Nullable
  abstract String getEventExtras();

  // @Nullable
  // abstract Map<String, Object> getUserProperties();

  abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_AmplitudeEvent.Builder();
  }

  public JSONObject toJson() {
    JSONObject json = new JSONObject();
    json.put("user_id", this.getUserId());

    JSONObject eventExtras = new JSONObject();
    eventExtras.put("extras", new JSONObject(this.getEventExtras()));
    json.put("event_properties", eventExtras);

    if (this.getAppVersion() != null) {
      json.put("app_version", this.getAppVersion());
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

    public abstract Builder setEventExtras(String newEventExtras);

    // public abstract Builder setUserProperties(Map<String, Object> newUserProperties);

    public abstract Builder setAppVersion(String newAppVersion);

    public abstract AmplitudeEvent build();
  }
}
