package com.mozilla.telemetry.transforms;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Container for transforms that decode PubsubMessage from various input formats.
 */
public class DecodePubsubMessages {

  /*
   * Static factory methods.
   */

  /** Decoder from non-json text to PubsubMessage. */
  public static PTransform<PCollection<? extends String>, PCollection<PubsubMessage>> text() {
    return PTransform.compose("DecodePubsubMessages.Text",
        input -> input.apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via((String s) -> new PubsubMessage(s.getBytes(StandardCharsets.UTF_8), null))));
  }

  /** Decoder from json to PubsubMessage. */
  public static PTransform<PCollection<? extends String>, PCollection<PubsubMessage>> json() {
    return PTransform.compose("DecodePubsubMessages.Json", input -> input
        .apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((String s) -> {
          try {
            return com.mozilla.telemetry.util.Json.readPubsubMessage(s);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        })));
  }

}
