/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.core.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;

public class Json {

  /**
   * A Jackson ObjectMapper pre-configured for use in com.mozilla.telemetry.
   */
  @VisibleForTesting
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Serialize {@code data} as a {@code byte[]}.
   *
   * @exception IOException if data cannot be encoded as json.
   */
  public static byte[] asBytes(Object data) throws IOException {
    return MAPPER.writeValueAsBytes(data);
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
}
