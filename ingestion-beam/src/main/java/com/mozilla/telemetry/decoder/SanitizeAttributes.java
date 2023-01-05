package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.PipelineMetadata;
import com.mozilla.telemetry.schema.SchemaStoreSingletonFactory;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Time;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Alter attributes of the message before sending to output, configurable per docType.
 *
 * <p>This is generally useful for dropping or reducing the granularity of data that may be overly
 * identifying, particularly when it comes to correlating datasets that are meant to be using
 * non-correlatable identifiers.
 *
 * <p>See https://bugzilla.mozilla.org/show_bug.cgi?id=1742172
 */
public class SanitizeAttributes
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final String schemasLocation;
  private transient PipelineMetadataStore pipelineMetadataStore;

  public static SanitizeAttributes of(String schemasLocation) {
    return new SanitizeAttributes(schemasLocation);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(ParDo.of(new Fn()));
  }

  ////////

  private SanitizeAttributes(String schemasLocation) {
    this.schemasLocation = schemasLocation;
  }

  private class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    @Setup
    public void setup() {
      pipelineMetadataStore = SchemaStoreSingletonFactory.getPipelineMetadataStore(schemasLocation);
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage message, OutputReceiver<PubsubMessage> out) {
      message = PubsubConstraints.ensureNonNull(message);
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      final PipelineMetadata meta = pipelineMetadataStore.getSchema(attributes);

      if (meta.submission_timestamp_granularity() != null) {
        // The pipeline metadata accepts lower-case values like "seconds", but the elements of
        // the ChronoUnit enum are uppercase, so we uppercase before lookup.
        String granularity = meta.submission_timestamp_granularity().toUpperCase();
        Instant instant = Time.parseAsInstantOrNull(attributes.get(Attribute.SUBMISSION_TIMESTAMP));
        if (instant != null) {
          attributes.put(Attribute.SUBMISSION_TIMESTAMP, DateTimeFormatter.ISO_INSTANT
              .format(instant.truncatedTo(ChronoUnit.valueOf(granularity))));
        }
      }

      if (meta.override_attributes() != null) {
        meta.override_attributes().forEach(override -> {
          if (override.value() == null) {
            attributes.remove(override.name());
          } else {
            attributes.put(override.name(), override.value());
          }
        });
      }

      out.output(new PubsubMessage(message.getPayload(), attributes));
    }
  }
}
