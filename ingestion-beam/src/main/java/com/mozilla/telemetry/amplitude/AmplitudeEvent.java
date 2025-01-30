package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class AmplitudeEvent implements Serializable {

  abstract String getUserId();

  abstract String getEventType();

  abstract long getTime();

  @Nullable
  abstract String getAppVersion();

  @Nullable
  abstract JsonNode getEventProperties();

  // @Nullable
  // abstract Map<String, Object> getUserProperties();

  public static Builder builder() {
    return new AutoValue_AmplitudeEvent.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setUserId(String newUserId);

    public abstract Builder setEventType(String newEventType);

    public abstract Builder setTime(long newTime);

    public abstract Builder setEventProperties(JsonNode newEventProperties);

    // public abstract Builder setUserProperties(Map<String, Object> newUserProperties);

    public abstract Builder setAppVersion(String newAppVersion);

    public abstract AmplitudeEvent build();
  }
}
