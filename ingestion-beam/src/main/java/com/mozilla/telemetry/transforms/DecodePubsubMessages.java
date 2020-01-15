package com.mozilla.telemetry.transforms;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * Base class for decoding PubsubMessage from various input formats.
 *
 * <p>Subclasses are provided via static members fromText() and fromJson().
 * We also provide an alreadyDecoded() transform for creating a PCollectionTuple
 * analogous to fromText or fromJson to handle output from PubsubIO.
 *
 * <p>The packaging of subclasses here follows the style guidelines as captured in
 * https://beam.apache.org/contribute/ptransform-style-guide/#packaging-a-family-of-transforms
 */
public abstract class DecodePubsubMessages
    extends MapElementsWithErrors.ToPubsubMessageFrom<String> {

  // Force use of static factory methods.
  private DecodePubsubMessages() {
  }

  /*
   * Static factory methods.
   */

  /** Decoder from non-json text to PubsubMessage. */
  public static Text text() {
    return new Text();
  }

  /** Decoder from json to PubsubMessage. */
  public static Json json() {
    return new Json();
  }

  /** Decoder from sanitized landfill JSON format to PubsubMessage. */
  public static SantizedLandfill sanitizedLandfill() {
    return new SantizedLandfill();
  }

  /*
   * Concrete subclasses.
   */

  public static class Text extends DecodePubsubMessages {

    protected PubsubMessage processElement(String element) {
      return new PubsubMessage(element.getBytes(StandardCharsets.UTF_8), null, null);
    }
  }

  public static class Json extends DecodePubsubMessages {

    protected PubsubMessage processElement(String element) throws IOException {
      return com.mozilla.telemetry.util.Json.readPubsubMessage(element);
    }
  }

  public static class SantizedLandfill extends DecodePubsubMessages {

    protected PubsubMessage processElement(String element) throws IOException {
      Map<String, String> attributes = new HashMap<>();
      ObjectNode root = com.mozilla.telemetry.util.Json
          .readObjectNode(element.getBytes(StandardCharsets.UTF_8));
      ObjectNode meta = (ObjectNode) root.path("meta");
      meta.fields().forEachRemaining(entry -> {
        String attribute = entry.getKey().toLowerCase().replaceAll("-", "_");
        if (entry.getValue().isTextual()) {
          attributes.put(attribute, entry.getValue().asText());
        }
      });
      byte[] payload = (root.path("content").asText()).getBytes(StandardCharsets.UTF_8);
      return new PubsubMessage(payload, attributes, null);
    }
  }

}
