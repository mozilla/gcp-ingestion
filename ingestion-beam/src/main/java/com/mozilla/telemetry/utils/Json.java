/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;

public class Json {
  /**
   * A Jackson ObjectMapper pre-configured for use in com.mozilla.telemetry.
   *
   * <p>Registers {@link PubsubMessageMixin} for decoding to {@link PubsubMessage}.
   *
   * <p>Registers {@link JsonOrgModule} for decoding to json.org types.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .addMixIn(PubsubMessage.class, PubsubMessageMixin.class)
      .registerModule(new JsonOrgModule());

  /**
   * Read a {@link JSONObject} from a byte array.
   *
   * <p>Using a Jackson {@link ObjectMapper} here is better about not changing long integers to
   * strings than using {@code new JSONObject(new String(data)}, and accepts {@code byte[]} so that
   * we don't need to decode to a {@link String} first.
   *
   * @exception IOException if {@code data} does not contain a valid json object.
   */
  @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
  public static JSONObject readJSONObject(byte[] data) throws IOException {
    // Read data into a tree
    TreeNode tree = MAPPER.readTree(data);
    // Check that we have an object, because treeToValue won't
    if (!tree.isObject()) {
      throw new IOException("json value is not an object");
    }
    // Return a JSONObject created from tree
    return MAPPER.treeToValue(tree, JSONObject.class);
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
   *   "attributeMap": {"${key}": "${value}"...}
   * }
   * </pre>
   */
  @JsonPropertyOrder(alphabetic = true)
  private abstract static class PubsubMessageMixin {
    @JsonCreator
    public PubsubMessageMixin(
        @JsonProperty("payload") byte[] payload,
        @JsonProperty("attributeMap") Map<String, String> attributes
    ) { }
  }
}
