package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.MapElements.MapWithFailures;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.TypeDescriptor;

public class AddMetadata {

  private AddMetadata() {
  }

  /** Factory method to create mapper instance. */
  public static MapWithFailures<PubsubMessage, PubsubMessage, PubsubMessage> of() {
    return MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage msg) -> {
      msg = PubsubConstraints.ensureNonNull(msg);
      byte[] metadata;
      try {
        // Get attributes as bytes, throws IOException
        metadata = Json.asBytes(com.mozilla.telemetry.ingestion.core.transform.AddMetadata
            .attributesToMetadataPayload(msg.getAttributeMap()));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      byte[] mergedPayload = com.mozilla.telemetry.ingestion.core.transform.AddMetadata
          .mergedPayload(msg.getPayload(), metadata);
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
