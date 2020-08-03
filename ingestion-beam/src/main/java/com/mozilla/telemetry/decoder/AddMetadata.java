package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.ingestion.core.transform.NestedMetadata;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.io.UncheckedIOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.MapElements.MapWithFailures;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link PTransform} that adds metadata from attributes into the JSON payload.
 *
 * <p>This transform must come after {@link ParsePayload} to ensure any existing
 * "metadata" key in the payload has been removed. Otherwise, this transform could add a
 * duplicate key leading to invalid JSON.
 */
public class AddMetadata {

  private AddMetadata() {
  }

  /** Factory method to create mapper instance. */
  public static MapWithFailures<PubsubMessage, PubsubMessage, PubsubMessage> of() {
    return MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage msg) -> {
      msg = PubsubConstraints.ensureNonNull(msg);
      byte[] metadata = Json
          .asBytes(NestedMetadata.attributesToMetadataPayload(msg.getAttributeMap()));
      byte[] mergedPayload = NestedMetadata.mergedPayload(msg.getPayload(), metadata);
      return new PubsubMessage(mergedPayload, msg.getAttributeMap());
    }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((WithFailures.ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (UncheckedIOException e) {
            return FailureMessage.of(AddMetadata.class.getSimpleName(), //
                ee.element(), //
                ee.exception());
          }
        });
  }
}
