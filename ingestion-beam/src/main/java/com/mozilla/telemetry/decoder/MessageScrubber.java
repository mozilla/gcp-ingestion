package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.metrics.Metrics;

/**
 * This class is called in {@link ParsePayload} to check for known signatures of potentially
 * harmful data. The {@link #scrub(Map, ObjectNode)} method may throw an exception as a signal
 * to route the message to error output or to be dropped.
 */
public class MessageScrubber {

  private static final Map<String, String> IGNORED_NAMESPACES = ImmutableMap
      .<String, String>builder().put("com-turkcell-yaani", "1612933") //
      .put("org-mozilla-fenix-beta", "1612934") //
      .put("org-mozilla-vrbrowser-dev", "1614410") //
      .put("org-mozilla-fenix-performancetest", "1614412") //
      .put("org-mozilla-vrbrowser-wavevr", "1614411") //
      .build();

  private static final ImmutableSet<String> FIREFOX_ONLY_DOCTYPES = ImmutableSet.of("event", "main",
      "modules");

  /**
   * Inspect the contents of the message to check for known signatures of potentially harmful data.
   *
   * <p>May throw an exception as a signal to route the message to error output or to be dropped.
   */
  public static void scrub(Map<String, String> attributes, ObjectNode json)
      throws MessageShouldBeDroppedException, AffectedByBugException {

    final String namespace = attributes.get(Attribute.DOCUMENT_NAMESPACE);
    final String docType = attributes.get(Attribute.DOCUMENT_TYPE);
    final String appName = attributes.get(Attribute.APP_NAME);
    final String appVersion = attributes.get(Attribute.APP_VERSION);
    final String appUpdateChannel = attributes.get(Attribute.APP_UPDATE_CHANNEL);
    final String appBuildId = attributes.get(Attribute.APP_BUILD_ID);

    // Check for toxic data that should be dropped without sending to error output.
    if (ParseUri.TELEMETRY.equals(namespace) && "crash".equals(docType)
        && "nightly".equals(appUpdateChannel) && "20190719094503".equals(appBuildId) //
        && Optional.of(json) // payload.metadata.MozCrashReason
            .map(j -> j.path("payload").path("metadata").path("MozCrashReason").textValue())
            .filter(s -> s.contains("do not use eval with system privileges")) //
            .isPresent()) {
      throw new MessageShouldBeDroppedException("1567596");
    }
    if (ParseUri.TELEMETRY.equals(namespace) && "crash".equals(docType)
        && (("nightly".equals(appUpdateChannel)
            && (appVersion.startsWith("68") || appVersion.startsWith("69")))
            || ("beta".equals(appUpdateChannel) && appVersion.startsWith("68")))
        && Optional.of(json) // payload.metadata.RemoteType
            .map(j -> j.path("payload").path("metadata").path("RemoteType").textValue())
            .filter(s -> s.startsWith("webIsolated=")) //
            .isPresent()) {
      throw new MessageShouldBeDroppedException("1562011");
    }
    if (ParseUri.TELEMETRY.equals(namespace) && "bhr".equals(docType)
        && (appVersion.startsWith("68") || appVersion.startsWith("69")) //
        && Optional.of(json) // payload.hangs[].remoteType
            .map(j -> j.path("payload").path("hangs").elements()) //
            .map(Streams::stream).orElseGet(Stream::empty).map(j -> j.path("remoteType")) //
            .filter(JsonNode::isTextual) //
            .anyMatch(j -> j.textValue().startsWith("webIsolated="))) {
      throw new MessageShouldBeDroppedException("1562011");
    }

    // Check for unwanted data; these messages aren't thrown out, but this class of errors will be
    // ignored for most pipeline monitoring.
    if (IGNORED_NAMESPACES.containsKey(namespace)) {
      throw new UnwantedDataException(IGNORED_NAMESPACES.get(namespace));
    }

    if ("FirefoxOS".equals(appName)) {
      throw new UnwantedDataException("1618684");
    }

    // These document types receive a significant number of pings with malformed `build_id`s due to
    // third-party builds where `appName != "Firefox"`
    if (ParseUri.TELEMETRY.equals(namespace) && FIREFOX_ONLY_DOCTYPES.contains(docType)
        && !"Firefox".equals(appName)) {
      throw new UnwantedDataException("1592010");
    }

    // Check for other signatures that we want to send to error output, but which should appear
    // in normal pipeline monitoring.
    if (bug1489560Affected(attributes, json)) {
      // See also https://bugzilla.mozilla.org/show_bug.cgi?id=1614428
      throw new AffectedByBugException("1489560");
    }

    // Redactions (message is altered, but allowed through).
    if (bug1602844Affected(attributes)) {
      json.path("events").elements().forEachRemaining(event -> {
        JsonNode eventMapValues = event.path(5);
        if (eventMapValues.has("fxauid")) {
          ((ObjectNode) eventMapValues).replace("fxauid", NullNode.getInstance());
        }
        markBugCounter("1602844");
      });
    }

  }

  private static void markBugCounter(String bugNumber) {
    Metrics.counter(MessageScrubber.class, "bug_" + bugNumber).inc();
  }

  //// The set of exceptions thrown by MessageScrubber.

  /**
   * Base class for all exceptions thrown by this class.
   *
   * <p>Constructors are required to provide a bug number to aid interpretation of these
   * errors. The constructor also increments a per-bug counter metric.
   */
  abstract static class MessageScrubberException extends RuntimeException {

    MessageScrubberException(String bugNumber) {
      super(bugNumber);
      markBugCounter(bugNumber);
    }
  }

  /**
   * Special exception to signal that a message matches a specific signature that we know
   * is data we never wanted to ingest in the first place; we send to error output out of caution,
   * but pipeline monitoring will generally filter out this type of error.
   */
  static class UnwantedDataException extends MessageScrubberException {

    UnwantedDataException(String bugNumber) {
      super(bugNumber);
    }
  }

  /**
   * Special exception to signal that a message is affected by a specific bug and should
   * be written to error output.
   */
  static class AffectedByBugException extends MessageScrubberException {

    AffectedByBugException(String bugNumber) {
      super(bugNumber);
    }
  }

  /**
   * Special exception class that signals that a given message should not be sent
   * downstream to either success or error output.
   */
  static class MessageShouldBeDroppedException extends MessageScrubberException {

    MessageShouldBeDroppedException(String bugNumber) {
      super(bugNumber);
    }
  }

  //// Private methods for checking for specific bug signatures.

  // see bug 1489560
  private static boolean bug1489560Affected(Map<String, String> attributes, ObjectNode json) {
    final String affectedClientId = "c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0";
    Map<String, String> tempAttributes = Maps.newHashMap();
    ParsePayload.addClientIdFromPayload(tempAttributes, json);

    return affectedClientId.equals(tempAttributes.get(Attribute.CLIENT_ID));
  }

  // See bug 1603487 for discussion of affected versions, etc.
  @VisibleForTesting
  static boolean bug1602844Affected(Map<String, String> attributes) {
    return ParseUri.TELEMETRY.equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && "focus-event".equals(attributes.get(Attribute.DOCUMENT_TYPE))
        && "Lockbox".equals(attributes.get(Attribute.APP_NAME))
        && attributes.get(Attribute.APP_VERSION) != null
        && ("1.7.0".equals(attributes.get(Attribute.APP_VERSION))
            || attributes.get(Attribute.APP_VERSION).matches("1\\.[0-6][0-9.]*"));
  }

}
