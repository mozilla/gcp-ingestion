package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Populate some counter metrics; payload passes through unchanged.
 */
public class EmitCounters
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static EmitCounters of() {
    return new EmitCounters();
  }

  private EmitCounters() {
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> messages) {
    return messages.apply(
        MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);
          Map<String, String> attributes = message.getAttributeMap();

          PerDocTypeCounter.inc(attributes, "valid_submission");
          if (attributes.containsKey(Attribute.REQUEST_ID)) {
            PerDocTypeCounter.inc(attributes, "valid_submission_merino");
          }
          return message;
        }));
  }

}
