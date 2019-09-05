/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Streams;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

public class MessageScrubber {

  private static final Counter countScrubbedBug1567596 = Metrics.counter(MessageScrubber.class,
      "bug_1567596");
  private static final Counter countScrubbedBug1562011 = Metrics.counter(MessageScrubber.class,
      "bug_1562011");

  /**
   * Inspect the contents of the payload and return true if the content matches a known pattern
   * we want to scrub and the message should not be sent downstream.
   *
   * <p>This is usually due to some potential for PII having leaked into the payload.
   */
  public static boolean shouldScrub(Map<String, String> attributes, JsonNode json) {
    // https://bugzilla.mozilla.org/show_bug.cgi?id=1567596
    if (ParseUri.TELEMETRY.equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && "crash".equals(attributes.get(Attribute.DOCUMENT_TYPE))
        && "nightly".equals(attributes.get(Attribute.APP_UPDATE_CHANNEL))
        && "20190719094503".equals(attributes.get(Attribute.APP_BUILD_ID)) //
        && Optional
            .ofNullable(json.path("payload").path("metadata").path("MozCrashReason").textValue())
            .map(text -> text.contains("do not use eval with system privileges")).orElse(false)) {
      countScrubbedBug1567596.inc();
      return true;
    } else if (ParseUri.TELEMETRY.equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && "crash".equals(attributes.get(Attribute.DOCUMENT_TYPE))
        && (("nightly".equals(attributes.get(Attribute.APP_UPDATE_CHANNEL))
            && (attributes.get(Attribute.APP_VERSION).startsWith("68")
                || attributes.get(Attribute.APP_VERSION).startsWith("69")))
            || ("beta".equals(attributes.get(Attribute.APP_UPDATE_CHANNEL))
                && attributes.get(Attribute.APP_VERSION).startsWith("68")))
        && Optional.ofNullable(json.path("payload").path("metadata").path("RemoteType").textValue())
            .map(text -> text.startsWith("webIsolated=")).orElse(false)) {
      // https://bugzilla.mozilla.org/show_bug.cgi?id=1562011
      countScrubbedBug1562011.inc();
      return true;
    } else if (ParseUri.TELEMETRY.equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && "bhr".equals(attributes.get(Attribute.DOCUMENT_TYPE))
        && (attributes.get(Attribute.APP_VERSION).startsWith("68")
            || attributes.get(Attribute.APP_VERSION).startsWith("69"))
        && Streams.stream(json.path("payload").path("hangs").elements())
            .anyMatch(node -> Optional.ofNullable(node.path("remoteType").textValue())
                .map(text -> text.startsWith("webIsolated=")).orElse(false))) {
      countScrubbedBug1562011.inc();
      return true;
    } else {
      return false;
    }
  }

}
