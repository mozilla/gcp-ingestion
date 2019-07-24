/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.json.JSONArray;
import org.json.JSONObject;

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
  public static boolean shouldScrub(Map<String, String> attributes, JSONObject json) {
    // https://bugzilla.mozilla.org/show_bug.cgi?id=1567596
    if ("telemetry".equals(attributes.get(ParseUri.DOCUMENT_NAMESPACE))
        && "crash".equals(attributes.get(ParseUri.DOCUMENT_TYPE))
        && "nightly".equals(attributes.get(ParseUri.APP_UPDATE_CHANNEL))
        && "20190719094503".equals(attributes.get(ParseUri.APP_BUILD_ID)) //
        && Optional.of(json) //
            .map(j -> j.optJSONObject("payload")) //
            .map(j -> j.optJSONObject("metadata")) //
            .map(j -> j.optString("MozCrashReason"))
            .filter(s -> s.contains("do not use eval with system privileges")).isPresent()) {
      countScrubbedBug1567596.inc();
      return true;
    } else if ("telemetry".equals(attributes.get(ParseUri.DOCUMENT_NAMESPACE))
        && "crash".equals(attributes.get(ParseUri.DOCUMENT_TYPE))
        && (("nightly".equals(attributes.get(ParseUri.APP_UPDATE_CHANNEL))
            && (attributes.get(ParseUri.APP_VERSION).startsWith("68")
                || attributes.get(ParseUri.APP_VERSION).startsWith("69")))
            || ("beta".equals(attributes.get(ParseUri.APP_UPDATE_CHANNEL))
                && attributes.get(ParseUri.APP_VERSION).startsWith("68")))
        && Optional.of(json) // payload.metadata.RemoteType
            .map(j -> j.optJSONObject("payload")) //
            .map(j -> j.optJSONObject("metadata")) //
            .map(j -> j.optString("RemoteType")) //
            .filter(s -> s.startsWith("webIsolated=")).isPresent()) {
      // https://bugzilla.mozilla.org/show_bug.cgi?id=1562011
      countScrubbedBug1562011.inc();
      return true;
    } else if ("telemetry".equals(attributes.get(ParseUri.DOCUMENT_NAMESPACE))
        && "bhr".equals(attributes.get(ParseUri.DOCUMENT_TYPE))
        && (attributes.get(ParseUri.APP_VERSION).startsWith("68")
            || attributes.get(ParseUri.APP_VERSION).startsWith("69"))
        && Optional.of(json) // payload.hangs[].remoteType
            .map(j -> j.optJSONObject("payload")) //
            .map(j -> {
              Boolean isPresent = false;
              JSONArray arr = j.optJSONArray("hangs");
              for (int i = 0; i < arr.length(); i++) {
                String s = arr.optJSONObject(i).optString("remoteType");
                if (s != null && s.startsWith("webIsolated=")) {
                  isPresent = true;
                  break;
                }
              }
              return isPresent;
            }) //
            .filter(b -> b).isPresent()) {
      countScrubbedBug1562011.inc();
      return true;
    } else {
      return false;
    }
  }

}
