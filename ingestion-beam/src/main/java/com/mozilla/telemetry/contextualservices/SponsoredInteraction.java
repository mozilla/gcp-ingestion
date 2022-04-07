package com.mozilla.telemetry.contextualservices;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class SponsoredInteraction implements Serializable {

  // These match the values from doctype
  // topsites, quicksuggest
  public static final String SOURCE_TOPSITES = "topsites";
  public static final String SOURCE_SUGGEST = "quicksuggest";

  @Nullable
  abstract String getSource();

  // desktop, phone, tablet
  public static final String FORM_DESKTOP = "desktop";
  public static final String FORM_PHONE = "phone";
  public static final String FORM_TABLET = "tablet";

  @Nullable
  abstract String getFormFactor();

  // impression, click
  public static final String INTERACTION_IMPRESSION = "impression";
  public static final String INTERACTION_CLICK = "click";

  @Nullable
  abstract String getInteractionType();

  @Nullable
  abstract String getContextId();

  @Nullable
  abstract String getReportingUrl();

  @Nullable
  abstract String getRequestId();

  @Nullable
  abstract String getSubmissionTimestamp();

  abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_SponsoredInteraction.Builder();
  }

  public String getDocumentType() {
    return String.format("%s-%s", getSource(), getInteractionType());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSource(String newSource);

    public abstract Builder setFormFactor(String newFormFactor);

    public abstract Builder setInteractionType(String newInteractionType);

    public abstract Builder setContextId(String newContextId);

    public abstract Builder setReportingUrl(String newReportingUrl);

    public abstract Builder setRequestId(String newRequestId);

    public abstract Builder setSubmissionTimestamp(String newSubmissionTimestamp);

    public abstract SponsoredInteraction build();
  }

}
