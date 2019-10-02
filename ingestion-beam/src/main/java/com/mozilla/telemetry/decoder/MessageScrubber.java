package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Streams;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
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
  public static boolean shouldScrub(Map<String, String> attributes, ObjectNode json) {
    // https://bugzilla.mozilla.org/show_bug.cgi?id=1567596
    if (ParseUri.TELEMETRY.equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && "crash".equals(attributes.get(Attribute.DOCUMENT_TYPE))
        && "nightly".equals(attributes.get(Attribute.APP_UPDATE_CHANNEL))
        && "20190719094503".equals(attributes.get(Attribute.APP_BUILD_ID)) //
        && Optional.of(json) // payload.metadata.MozCrashReason
            .map(j -> j.path("payload").path("metadata").path("MozCrashReason").textValue())
            .filter(s -> s.contains("do not use eval with system privileges")) //
            .isPresent()) {
      countScrubbedBug1567596.inc();
      return true;
    } else if (ParseUri.TELEMETRY.equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && "crash".equals(attributes.get(Attribute.DOCUMENT_TYPE))
        && (("nightly".equals(attributes.get(Attribute.APP_UPDATE_CHANNEL))
            && (attributes.get(Attribute.APP_VERSION).startsWith("68")
                || attributes.get(Attribute.APP_VERSION).startsWith("69")))
            || ("beta".equals(attributes.get(Attribute.APP_UPDATE_CHANNEL))
                && attributes.get(Attribute.APP_VERSION).startsWith("68")))
        && Optional.of(json) // payload.metadata.RemoteType
            .map(j -> j.path("payload").path("metadata").path("RemoteType").textValue())
            .filter(s -> s.startsWith("webIsolated=")) //
            .isPresent()) {
      // https://bugzilla.mozilla.org/show_bug.cgi?id=1562011
      countScrubbedBug1562011.inc();
      return true;
    } else if (ParseUri.TELEMETRY.equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && "bhr".equals(attributes.get(Attribute.DOCUMENT_TYPE))
        && (attributes.get(Attribute.APP_VERSION).startsWith("68")
            || attributes.get(Attribute.APP_VERSION).startsWith("69"))
        && Optional.of(json) // payload.hangs[].remoteType
            .map(j -> j.path("payload").path("hangs").elements()) //
            .map(Streams::stream).orElseGet(Stream::empty).map(j -> j.path("remoteType")) //
            .filter(JsonNode::isTextual) //
            .anyMatch(j -> j.textValue().startsWith("webIsolated="))) {
      countScrubbedBug1562011.inc();
      return true;
    } else {
      return false;
    }
  }

}
