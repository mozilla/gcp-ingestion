/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.transform;

import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.util.KV;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RouteMessage {

  RouteMessage() {
  }

  private static final ZoneId UTC = ZoneId.of("UTC");

  private static String now() {
    return ZonedDateTime.now(UTC).format(DateTimeFormatter.ISO_DATE_TIME);
  }

  public static KV<String, PubsubMessage> apply(PubsubMessage message) {
    String timestamp = Optional.ofNullable(message.getAttributesMap())
        .flatMap(attrs -> Optional.ofNullable(attrs.get("submission_timestamp")))
        .orElseGet(RouteMessage::now);
    String key = timestamp.substring(0, 13).replace('-', '/').replace('T', '/');
    return new KV<>(key, message);
  }
}
