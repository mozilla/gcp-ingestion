package com.mozilla.telemetry.aet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

public class SanitizeJsonPayload {

  /**
   * Return a redacted value if the given node represents a long string.
   *
   * <p>Strings longer than 32 characters might be ecosystem_anon_id values, so we want to make
   * sure they are not propagated to error output.
   */
  private static JsonNode redactedNode(JsonNode node) {
    String value = node.textValue();
    if (value != null && value.length() > 32) {
      return TextNode.valueOf(
          String.format("%s<%d characters redacted>", value.substring(0, 4), value.length() - 4));
    } else {
      return node;
    }
  }

  /**
   * Recursively walk a JSON tree, redacting long string values.
   */
  @VisibleForTesting
  static void sanitizeJsonNode(JsonNode node) {
    if (node.isObject()) {
      List<Entry<String, JsonNode>> fields = Lists.newArrayList(node.fields());
      for (Map.Entry<String, JsonNode> entry : fields) {
        String fieldName = entry.getKey();
        JsonNode value = entry.getValue();
        ((ObjectNode) node).set(fieldName, redactedNode(value));
      }
    }
    if (node.isArray()) {
      List<JsonNode> elements = Lists.newArrayList(node.elements());
      for (int i = 0; i < elements.size(); i++) {
        JsonNode value = elements.get(i);
        if (value.isTextual() && value.asText().length() > 32) {
          ((ArrayNode) node).set(i, redactedNode(value));
        }
      }
    }

    // Recursively sanitize objects and arrays.
    if (node.isContainerNode()) {
      node.elements().forEachRemaining(SanitizeJsonPayload::sanitizeJsonNode);
    }
  }

  /** Return SantizeJsonPayload transform. */
  public static MapElements<PubsubMessage, PubsubMessage> of() {
    return MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage message) -> {
      byte[] sanitizedPayload;
      try {
        ObjectNode json = Json.readObjectNode(message.getPayload());
        sanitizeJsonNode(json);
        sanitizedPayload = Json.asBytes(json);
      } catch (IOException ignore) {
        sanitizedPayload = null;
      }
      return new PubsubMessage(sanitizedPayload, message.getAttributeMap());
    });
  }

}
