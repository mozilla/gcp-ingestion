package com.mozilla.telemetry.contextualservices;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import javax.annotation.Nullable;
import java.io.Serializable;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class SponsoredInteraction implements Serializable {
    // These match the values from doctype
    // topsites, quicksuggest
    public static final String SOURCE_TOPSITE = "topsites";
    public static final String SOURCE_SUGGEST = "quicksuggest";
    @Nullable
    abstract String source();

    // desktop, phone, tablet
    public static final String FORM_DESKTOP = "desktop";
    public static final String FORM_PHONE = "phone";
    public static final String FORM_TABLET = "tablet";
    @Nullable
    abstract String form();

    // impression, click
    public static final String INTERACTION_IMPRESSION = "impression";
    public static final String INTERACTION_CLICK = "click";
    @Nullable
    abstract String interaction();

    @Nullable
    abstract String contextID();

    @Nullable
    abstract String reporterURL();

    @Nullable
    abstract String requestID();

    @Nullable
    abstract String submissionTimestamp();

    abstract Builder toBuilder();

    public static Builder builder() {
        return new AutoValue_SponsoredInteraction.Builder();
    }

    public String getDocumentType() {
        return String.format("%s-%s", source(), interaction());
    }

    @AutoValue.Builder
    public abstract static class Builder {

        public abstract Builder reporterURL(String reporterURL);

        public abstract Builder contextID(String contextID);

        public abstract Builder requestID(String requestID);

        public abstract Builder source(String source);

        public abstract Builder form(String form);

        public abstract Builder interaction(String interaction);

        public abstract Builder submissionTimestamp(String submissionTimestamp);

        public abstract SponsoredInteraction build();
    }

}
