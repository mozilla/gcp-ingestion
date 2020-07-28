package com.mozilla.telemetry.ingestion.sink.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.Constant.FieldName;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;

public class StringToPubsubMessage {

  /** Never instantiate this class. */
  private StringToPubsubMessage() {
  }

  private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

  /** Construct a PubsubMessage from JSON. */
  public static PubsubMessage apply(String line) {
    try {
      ObjectNode node = Json.readObjectNode(line);
      PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder();
      JsonNode dataNode = node.path(FieldName.PAYLOAD);
      if (!dataNode.isMissingNode() && !dataNode.isNull()) {
        final byte[] data;
        if (dataNode.isTextual()) {
          data = BASE64_DECODER.decode(dataNode.asText());
        } else {
          data = Json.asBytes(dataNode);
        }
        messageBuilder.setData(ByteString.copyFrom(data));
      }
      JsonNode attributeNode = node.path("attributeMap");
      if (attributeNode.isObject()) {
        attributeNode.fields().forEachRemaining(
            entry -> messageBuilder.putAttributes(entry.getKey(), entry.getValue().asText()));
      }
      return messageBuilder.build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
