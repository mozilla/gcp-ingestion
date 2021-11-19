package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.PipelineMetadata;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.Time;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class SanitizeAttributes
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final String schemasLocation;
  private transient PipelineMetadataStore pipelineMetadataStore;

  public static SanitizeAttributes of(String schemasLocation) {
    return new SanitizeAttributes(schemasLocation);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }

  ////////

  private SanitizeAttributes(String schemasLocation) {
    this.schemasLocation = schemasLocation;
  }

  private class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      message = PubsubConstraints.ensureNonNull(message);
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      if (pipelineMetadataStore == null) {
        pipelineMetadataStore = PipelineMetadataStore.of(schemasLocation,
            BeamFileInputStream::open);
      }

      final PipelineMetadata meta = pipelineMetadataStore.getSchema(attributes);
      if (meta.submission_timestamp_granularity() != null) {
        String granularity = meta.submission_timestamp_granularity().toUpperCase();
        Instant instant = Time.parseAsInstantOrNull(attributes.get(Attribute.SUBMISSION_TIMESTAMP));
        if (instant != null) {
          attributes.put(Attribute.SUBMISSION_TIMESTAMP, DateTimeFormatter.ISO_INSTANT
              .format(instant.truncatedTo(ChronoUnit.valueOf(granularity))));
        }
      }

      return new PubsubMessage(message.getPayload(), attributes);
    }
  }

  private final Fn fn = new Fn();

}
