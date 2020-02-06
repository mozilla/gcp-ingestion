package com.mozilla.telemetry.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * Extends {@link com.mozilla.telemetry.ingestion.core.util.Json} with configuration and methods
 * specific to ingestion-beam.
 *
 * <p>Registers {@link PubsubMessageMixin} for decoding to {@link PubsubMessage}.
 *
 * <p>Registers {@link JsonOrgModule} for decoding to json.org types.
 */
public class Json extends com.mozilla.telemetry.ingestion.core.util.Json {

  static {
    MAPPER.addMixIn(PubsubMessage.class, PubsubMessageMixin.class);
  }

  /**
   * Make serialization of {@link Map} deterministic for testing.
   */
  @VisibleForTesting
  static void enableOrderMapEntriesByKeys() {
    Json.MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  /**
   * Reset the {@link ObjectMapper} configuration changed above.
   */
  @VisibleForTesting
  static void disableOrderMapEntriesByKeys() {
    Json.MAPPER.disable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  /**
   * Read a {@link TableRow} from a byte array.
   *
   * @exception IOException if {@code data} does not contain a valid {@link TableRow}.
   */
  public static TableRow readTableRow(byte[] data) throws IOException {
    if (data == null) {
      throw new IOException("cannot decode null byte array to TableRow");
    }
    // Throws IOException
    TableRow output = Json.MAPPER.readValue(data, TableRow.class);
    if (output == null) {
      throw new IOException("not a valid TableRow: null");
    }
    return output;
  }

  /**
   * Read bytes into a tree of {@link com.fasterxml.jackson.databind.JsonNode}.
   *
   * @exception IOException if {@code data} does not contain a valid json object.
   */
  public static ObjectNode readObjectNode(byte[] data) throws IOException {
    // Read data into a tree
    TreeNode root = MAPPER.readTree(data);
    // Check that we have an object, because treeToValue won't
    if (root == null || !root.isObject()) {
      throw new IOException("json value is not an object");
    }
    return (ObjectNode) root;
  }

  /**
   * Read bytes into an {@link ArrayNode}.
   *
   * @exception IOException if {@code data} does not contain a valid json object.
   */
  public static ArrayNode readArrayNode(byte[] data) throws IOException {
    // Read data into a tree
    TreeNode root = MAPPER.readTree(data);
    // Check that we have an array, because treeToValue won't
    if (root == null || !root.isArray()) {
      throw new IOException("json value is not an array");
    }
    return (ArrayNode) root;
  }

  /**
   * Return a new, empty {@link ObjectNode}.
   */
  public static ObjectNode createObjectNode() {
    return MAPPER.createObjectNode();
  }

  /**
   * Return a new, empty {@link ArrayNode}.
   */
  public static ArrayNode createArrayNode() {
    return MAPPER.createArrayNode();
  }

  /**
   * Read a {@link PubsubMessage} from a string.
   *
   * @exception IOException if {@code data} does not contain a valid {@link PubsubMessage}.
   */
  public static PubsubMessage readPubsubMessage(String data) throws IOException {
    PubsubMessage output = MAPPER.readValue(data, PubsubMessage.class);

    if (output == null) {
      throw new IOException("not a valid PubsubMessage: null");
    } else if (output.getPayload() == null) {
      throw new IOException("not a valid PubsubMessage.payload: null");
    }
    return output;
  }

  /**
   * Use {@code MAPPER} to convert {@link ObjectNode} to an arbitrary class.
   *
   * @throws JsonProcessingException if the conversion is unsuccessful
   */
  public static <T> T convertValue(ObjectNode root, Class<T> klass) throws JsonProcessingException {
    return MAPPER.treeToValue(root, klass);
  }

  /**
   * Serialize {@code data} as a {@link String}.
   *
   * @exception IOException if data cannot be encoded as json.
   */
  public static String asString(Object data) throws IOException {
    return MAPPER.writeValueAsString(data);
  }

  /**
   * Jackson mixin for decoding {@link PubsubMessage} from json.
   *
   * <p>This is necessary because jackson can automatically determine how to encode
   * {@link PubsubMessage} as json, but it can't automatically tell how to decode it because the
   * {@code getAttributeMap} method returns the value for the {@code attributes} parameter.
   * Additionally jackson doesn't like that there are no setter methods on {@link PubsubMessage}.
   *
   * <p>The default jackson output format for PubsubMessage, which we want to read, looks like:
   * <pre>
   * {
   *   "payload": "${base64 encoded byte array}",
   *   "attributeMap": {"${key}": "${value}"...},
   *   "messageId": "${id}"
   * }
   * </pre>
   */
  @JsonPropertyOrder(alphabetic = true)
  private abstract static class PubsubMessageMixin {

    @JsonCreator
    public PubsubMessageMixin(@JsonProperty("payload") byte[] payload,
        @JsonProperty("attributeMap") Map<String, String> attributes,
        @JsonProperty("messageId") String messageId) {
    }

    // Ignore messageId when serializing to JSON, but read during deserialization.
    @JsonIgnore
    abstract String getMessageId();
  }
}
