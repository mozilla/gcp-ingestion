/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.util;

import com.google.pubsub.v1.PubsubMessage;
import java.util.Base64;
import org.json.JSONObject;

/** Json transform(s). */
public class Json {

  /** Serialize {@code message} as a {@link String}. */
  public static String asString(PubsubMessage message) {
    return new JSONObject()
        .put("payload", Base64.getEncoder().encodeToString(message.getData().toByteArray()))
        .put("attributesMap", message.getAttributesMap()).toString();
  }
}
