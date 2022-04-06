package com.mozilla.telemetry.contextualservices;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

@AutoValue
public abstract class SponsoredInteraction implements Serializable {
    // topsites, suggest
    public static final String SOURCE_TOPSITE = "topsites";
    public static final String SOURCE_SUGGEST = "suggest";
    abstract String source();

    // desktop, phone, tablet
    public static final String FORM_DESKTOP = "desktop";
    public static final String FORM_PHONE = "phone";
    public static final String FORM_TABLET = "tablet";
    abstract String form();

    // impression, click
    public static final String INTERACTION_IMPRESSION = "impression";
    public static final String INTERACTION_CLICK = "click";
    abstract String interaction();

    abstract String contextID();

    @Nullable
    abstract String reporterURL();

    @Nullable
    abstract String requestID();

    @Nullable
    abstract String submissionTimestamp();

    public static Builder builder() {
        return new AutoValue_SponsoredInteraction.Builder();
    }

    public String getDocumentType() {
        return String.format("%s-%s", source(), interaction());
    }

    public Map<String, String> toMap() {
        return Map.of(
            "reporter-url", reporterURL(),
            "context-id", contextID(),
            "request-id", requestID(),
            "source", source(),
            "form", form(),
            "interaction", interaction(),
            "submission-timestamp", submissionTimestamp()
        );
    }

    public static SponsoredInteraction of(Map<String, String> map) {
        return SponsoredInteraction.builder()
                .reporterURL(map.get("reporter-url"))
                .contextID(map.get("context-id"))
                .requestID(map.get("request-id"))
                .source(map.get("source"))
                .form(map.get("form"))
                .interaction(map.get("interaction"))
                .submissionTimestamp(map.get("submission-timestamp"))
                .build();
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

        public Builder from(SponsoredInteraction interaction) {
            return new AutoValue_SponsoredInteraction.Builder()
                    .reporterURL(interaction.reporterURL())
                    .contextID(interaction.contextID())
                    .requestID(interaction.requestID())
                    .source(interaction.source())
                    .form(interaction.form())
                    .interaction(interaction.interaction())
                    .submissionTimestamp(interaction.submissionTimestamp());
        }
    }

}
