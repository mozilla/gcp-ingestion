/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.core.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

public class Json {

  /**
   * A Jackson ObjectMapper pre-configured for use in com.mozilla.telemetry.
   */
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Read a {@link ObjectNode} from a byte array.
   *
   * @exception IOException if {@code data} does not contain a valid {@link ObjectNode}.
   */
  public static ObjectNode readObjectNode(byte[] data) throws IOException {
    if (data == null) {
      throw new IOException("cannot decode null byte array to ObjectNode");
    }
    // Throws IOException
    ObjectNode output = MAPPER.readValue(data, ObjectNode.class);
    if (output == null) {
      throw new IOException("not a valid JsonNode: null");
    }
    return output;
  }

  /**
   * Use {@code MAPPER} to create an {@link ObjectNode}.
   */
  public static ObjectNode createObjectNode() {
    return MAPPER.createObjectNode();
  }

  /**
   * Serialize {@code data} as a {@code byte[]}.
   *
   * @exception IOException if data cannot be encoded as json.
   */
  public static byte[] asBytes(Object data) throws IOException {
    return MAPPER.writeValueAsBytes(data);
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
   * Use {@code MAPPER} to convert {@code Map<String, ?>} to {@link ObjectNode}.
   */
  public static ObjectNode asObjectNode(Map<String, ?> map) {
    return MAPPER.valueToTree(map);
  }

  /**
   * Use {@code MAPPER} to convert {@link ObjectNode} to {@code Map<String, Object>}.
   */
  public static Map<String, Object> asMap(ObjectNode objectNode) {
    return MAPPER.convertValue(objectNode, new TypeReference<Object>() {
    });
  }

  /**
   * Make serialization of {@link Map} deterministic for testing.
   */
  @VisibleForTesting
  public static void enableOrderMapEntriesByKeys() {
    Json.MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  /**
   * Reset the {@link ObjectMapper} configuration changed above.
   */
  @VisibleForTesting
  public static void disableOrderMapEntriesByKeys() {
    Json.MAPPER.disable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  /**
   * Reserialize {@code data} to make json deterministic for testing.
   */
  @VisibleForTesting
  public static String sortJson(String data) {
    try {
      // read value into an Object then Serialize as String to sort Map entries
      return Json.asString(Json.MAPPER.readValue(data, Object.class));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
