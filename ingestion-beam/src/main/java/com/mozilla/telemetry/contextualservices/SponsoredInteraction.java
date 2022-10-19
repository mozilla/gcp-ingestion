package com.mozilla.telemetry.contextualservices;

import com.google.auto.value.AutoValue;
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
public abstract class SponsoredInteraction implements Serializable {

  // These match the values from docType
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

  /**
   * Scenario can be either 'online' or 'offline'. The value is parsed from the payload field:
   * `improve_suggest_experience_check` which is a boolean that represents `online` if true and
   * `offline` if false.
   * @return String
   */
  @Nullable
  abstract String getScenario();

  // scenario values: online/offline
  public static final String ONLINE = "online";
  public static final String OFFLINE = "offline";

  @Nullable
  abstract String getInteractionType();

  @Nullable
  abstract String getContextId();

  @Nullable
  abstract String getReportingUrl();

  @Nullable
  abstract String getRequestId();

  @Nullable
  public abstract String getSubmissionTimestamp();

  @Nullable
  public abstract String getOriginalDocType();

  @Nullable
  public abstract String getOriginalNamespace();

  abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_SponsoredInteraction.Builder();
  }

  /**
   * Uses the original pubsub message document_type or if that is not available
   * it will build one based on `source` and `interactionType` fields.
   * @return String
   */
  public String getDerivedDocumentType() {
    return String.format("%s-%s", getSource(), getInteractionType());
  }

  /**
   * There are a few places that we can not use an inferred Coder. This provides a
   * SchemaCoder for the SponsoredInteraction class. Returns `null` if a schema cannot be found.
   *
   * @return The schema coder for a SponsoredInteraction.
   */
  public static Coder<SponsoredInteraction> getCoder() {
    AutoValueSchema autoValueSchema = new AutoValueSchema();
    TypeDescriptor<SponsoredInteraction> td = TypeDescriptor.of(SponsoredInteraction.class);
    Schema schema = autoValueSchema.schemaFor(td);
    if (schema != null) {
      return SchemaCoder.of(schema, td, autoValueSchema.toRowFunction(td),
          autoValueSchema.fromRowFunction(td));
    }
    return null;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSource(String newSource);

    public abstract Builder setFormFactor(String newFormFactor);

    public abstract Builder setScenario(String newScenario);

    public abstract Builder setInteractionType(String newInteractionType);

    public abstract Builder setContextId(String newContextId);

    public abstract Builder setReportingUrl(String newReportingUrl);

    public abstract Builder setRequestId(String newRequestId);

    public abstract Builder setSubmissionTimestamp(String newSubmissionTimestamp);

    public abstract Builder setOriginalDocType(String newOriginalDocType);

    public abstract Builder setOriginalNamespace(String newOriginalNamespace);

    public abstract SponsoredInteraction build();
  }

}
