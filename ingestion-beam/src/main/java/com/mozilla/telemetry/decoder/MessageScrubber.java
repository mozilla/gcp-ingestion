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
      .put("org-mozilla-fogotype", "1635592") //
      .put("com-pumabrowser-pumabrowser", "1637055") //
      .put("org-mozilla-fenix-debug", "1614409") //
      .put("com-granitamalta-cloudbrowser", "1644200") //
      .put("org-mozilla-fenix-tm", "1638902") //
      .put("network-novak-iceweasel", "1662737") //
      .put("skipity-web-browser-ios", "1662737") //
      .put("com-abaqoo-fenix", "1669281") //
      .put("ai-laso-ios-insight", "1669280") //
      .put("org-mozilla-fennec-fdroid", "1669279") //
      .put("io-uc-browser-web", "1671993") //
      .put("org-torproject-torbrowser-alpha", "1671987") //
      .put("com-nationaledtech-spin", "1675135") //
      .put("org-mozilla-ios-Lockbox-CredentialProvider", "1679830") //
      .put("com-ucbrowser-indian-browser", "1682425") //
      .put("com-athena-equality-firefox", "1686088") //
      .put("com-authenticatedreality-fireios", "1686087") //
      .put("com-netsweeper-clientfilter-lgfl-homeprotect", "1686085") //
      .put("com-myie9-xf", "1684918") //
      .put("com-netsweeper-clientfilter-lgfl-socialblocked", "1688689") //
      .put("com-only4free-activate", "1687350") //
      .put("glean-js-tmp", "1689513") //
      .put("org-privacywall-browser", "1691468") //
      .put("org-mozilla-ios-lockbox-credentialprovider", "1695728") //
      .put("com-zibb-browser", "1698576") //
      .put("org-mozilla-firefox-betb", "1698579") //
      .put("org-mozilla-allanchain-firefox", "1698574") //
      .put("com-nationaledtech-spinbrowser", "1708753") //
      .put("com-netsweeper-clientfilter-netsweeper", "1708754") //
      .put("com-searchscene-ios", "1715101") //
      .put("org-geocomply-ios-fennec", "1711513") //
      .put("org-mozilla-vrbrowser-internal", "1715099") //
      .put("com-qwant-mobile", "1717450") //
      .put("io-github-forkmaintainers-iceraven", "1717479") //
      .put("artistscope-ios-artisbrowser", "1719343") //
      .put("com-luxxle-ios-fennec", "1719341") //
      .put("com-etiantian-pclass", "1719338") //
      .put("com-apple-dt-xctest-tool", "1732313") //
      .put("org-mozilla-feniy", "1732313") //
      .put("org-mozilla-felix", "1737633") //
      .put("browser-oceanhero", "1740091") //
      .put("com-qebrowser", "1740091") //
      .put("org-mozilla-sorizava-focus", "1740091") //
      .build();

  private static final Map<String, String> IGNORED_TELEMETRY_DOCTYPES = ImmutableMap
      .<String, String>builder().put("pioneer-study", "1631849") //
      .put("frecency-update", "1633525") //
      .put("saved-session", "1656910") //
      .build();

  private static final ImmutableSet<String> FIREFOX_ONLY_DOCTYPES = ImmutableSet.of("event", "main",
      "modules");

  private static final Map<String, String> IGNORED_URIS = ImmutableMap.<String, String>builder()
      .put("/submit/sslreports", "1585144") //
      .put("/submit/sslreports/", "1585144") //
      .build();

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
    // NOTE: this value may be null
    final String userAgent = attributes.get(Attribute.USER_AGENT);

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
    if ("account-ecosystem".equals(docType)) {
      throw new MessageShouldBeDroppedException("1697602");
    }

    // Check for unwanted data; these messages aren't thrown out, but this class of errors will be
    // ignored for most pipeline monitoring.
    if (IGNORED_NAMESPACES.containsKey(namespace)) {
      throw new UnwantedDataException(IGNORED_NAMESPACES.get(namespace));
    }

    if (ParseUri.TELEMETRY.equals(namespace) && IGNORED_TELEMETRY_DOCTYPES.containsKey(docType)) {
      throw new UnwantedDataException(IGNORED_TELEMETRY_DOCTYPES.get(docType));
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

    // Glean enforces a particular user-agent string that a rogue fuzzer is not abiding by
    // https://searchfox.org/mozilla-central/source/third_party/rust/glean-core/src/upload/request.rs#35,72-75
    if ("firefox-desktop".equals(namespace)
        && (userAgent == null || !userAgent.startsWith("Glean"))) {
      throw new UnwantedDataException("1684980");
    }

    // Check for other signatures that we want to send to error output, but which should appear
    // in normal pipeline monitoring.
    if (bug1489560Affected(attributes, json)) {
      // See also https://bugzilla.mozilla.org/show_bug.cgi?id=1614428
      throw new AffectedByBugException("1489560");
    }

    // No such docType: default-browser-agent/1
    if ("default-browser-agent".equals(namespace) && "1".equals(docType)) {
      throw new AffectedByBugException("1626020");
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

    if (bug1162183Affected(attributes)) {
      JsonNode payload = json.path("payload");
      if (payload.has("slowSQL")) {
        ((ObjectNode) payload).remove("slowSQL");
        markBugCounter("1162183");
      }
    }

    if (bug1642386Affected(attributes)) {
      json.path("payload").path("syncs").elements().forEachRemaining(syncItem -> {
        syncItem.path("engines").elements().forEachRemaining(engine -> {
          ((ObjectNode) engine).remove("outgoing");
          markBugCounter("1642386");
        });
      });
    }

    if (bug1712850Affected(attributes)) {
      if (json.hasNonNull("search_query") || json.hasNonNull("matched_keywords")) {
        json.put("search_query", "");
        json.put("matched_keywords", "");
        markBugCounter("1712850");
      }
    }

    // Data collected prior to glean.js 0.17.0 is effectively useless.
    if (bug1733118Affected(namespace, docType, json)) {
      // See also https://bugzilla.mozilla.org/show_bug.cgi?id=1733118
      throw new AffectedByBugException("1733118");
    }

  }

  /**
   * Check the message URI against known URIs of messages with potentially harmful data.
   *
   * <p>May throw an exception as a signal to route the message to error output or to be dropped.
   */
  public static void scrubByUri(String uri) {
    if (IGNORED_URIS.containsKey(uri)) {
      throw new UnwantedDataException(IGNORED_URIS.get(uri));
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
            || attributes.get(Attribute.APP_VERSION).matches("^1\\.[0-6][0-9.]*"));
  }

  // See bug 1162183 for discussion of affected versions, etc.
  @VisibleForTesting
  static boolean bug1162183Affected(Map<String, String> attributes) {
    final ImmutableSet<String> affectedDocumentTypes = ImmutableSet.of("first-shutdown", "main",
        "saved-session");

    return ParseUri.TELEMETRY.equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && affectedDocumentTypes.contains(attributes.get(Attribute.DOCUMENT_TYPE))
        && attributes.get(Attribute.APP_VERSION) != null
        && attributes.get(Attribute.APP_VERSION).matches("^([0-3][0-9]|4[0-1])\\..*"); // >= 41
  }

  // See bug 1642386 for discussion of affected versions, etc.
  @VisibleForTesting
  static boolean bug1642386Affected(Map<String, String> attributes) {
    return ParseUri.TELEMETRY.equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && attributes.get(Attribute.DOCUMENT_TYPE).equals("sync")
        && attributes.get(Attribute.APP_VERSION) != null
        && attributes.get(Attribute.APP_VERSION).matches("^([0-9]|[0-2][0-7])\\..*"); // <= 27
  }

  // See bug 1712850
  @VisibleForTesting
  static boolean bug1712850Affected(Map<String, String> attributes) {
    return "contextual-services".equals(attributes.get(Attribute.DOCUMENT_NAMESPACE))
        && "quicksuggest-impression".equals(attributes.get(Attribute.DOCUMENT_TYPE));
  }

  // See bug 1733118 for discussion of affected versions, etc.
  private static boolean bug1733118Affected(String namespace, String docType, ObjectNode json) {
    return "mozillavpn".equals(namespace) && "main".equals(docType)
        && ParsePayload.getGleanClientInfo(json).path("telemetry_sdk_build").asText("")
            .matches("0[.]([0-9]|1[0-6])[.].*"); // < 0.17
  }

}
