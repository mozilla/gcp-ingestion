package com.mozilla.telemetry.decoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.mozilla.telemetry.decoder.MessageScrubber.AffectedByBugException;
import com.mozilla.telemetry.decoder.MessageScrubber.MessageShouldBeDroppedException;
import com.mozilla.telemetry.decoder.MessageScrubber.UnwantedDataException;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.util.Json;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class MessageScrubberTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testUnwantedDataBug1612933() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "com-turkcell-yaani")
        .put(Attribute.DOCUMENT_TYPE, "baseline").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1612934() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "org-mozilla-fenix-beta")
        .put(Attribute.DOCUMENT_TYPE, "baseline").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1614410() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "org-mozilla-vrbrowser-dev")
        .put(Attribute.DOCUMENT_TYPE, "baseline").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1614411() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "org-mozilla-fenix-performancetest")
        .put(Attribute.DOCUMENT_TYPE, "baseline").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1635592() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "org-mozilla-vrbrowser-wavevr")
        .put(Attribute.DOCUMENT_TYPE, "baseline").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1614412() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "org-mozilla-fogotype")
        .put(Attribute.DOCUMENT_TYPE, "baseline").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1644200() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "com-granitamalta-cloudbrowser")
        .put(Attribute.DOCUMENT_TYPE, "baseline").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1638902() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "org-mozilla-fenix-tm")
        .put(Attribute.DOCUMENT_TYPE, "baseline").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1618684() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.APP_NAME, "FirefoxOS").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1592010() {
    String[] unwantedApps = { "Ordissimo", "adloops", "agendissimo", "ZeroWeb", "CoreApp" };

    for (String app : unwantedApps) {
      Map<String, String> attributes = ImmutableMap.<String, String>builder()
          .put(Attribute.APP_NAME, app) //
          .put(Attribute.DOCUMENT_NAMESPACE, "telemetry") //
          .put(Attribute.DOCUMENT_TYPE, "main").build();

      assertThrows(UnwantedDataException.class,
          () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
    }
  }

  @Test
  public void testUnwantedDataBug1637055() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "com-pumabrowser-pumabrowser")
        .put(Attribute.DOCUMENT_TYPE, "baseline").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testShouldScrubBug1567596() throws Exception {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "telemetry").put(Attribute.DOCUMENT_TYPE, "crash")
        .put(Attribute.APP_UPDATE_CHANNEL, "nightly").put(Attribute.APP_BUILD_ID, "20190719094503")
        .put(Attribute.APP_VERSION, "70.0a1").build();
    ObjectNode bug1567596AffectedJson = Json.readObjectNode(("{\n" //
        + "  \"payload\": {\n" //
        + "    \"metadata\": {\n" //
        + "      \"MozCrashReason\": \"bar; do not use eval with system privileges foo)\"\n" //
        + "    },\n" //
        + "    \"session_id\": \"ca98fe03-1248-448f-bbdf-59f97dba5a0e\"\n" //
        + "  },\n" //
        + "  \"client_id\": null\n" + "}").getBytes(StandardCharsets.UTF_8));

    assertThrows(MessageShouldBeDroppedException.class,
        () -> MessageScrubber.scrub(attributes, bug1567596AffectedJson));

    MessageScrubber.scrub(new HashMap<>(), bug1567596AffectedJson);
    MessageScrubber.scrub(attributes, Json.createObjectNode());
  }

  @Test
  public void testShouldScrubCrashBug1562011() throws Exception {
    ObjectNode ping = Json.readObjectNode(("{\n" //
        + "  \"payload\": {\n" //
        + "    \"metadata\": {\n" //
        + "      \"RemoteType\": \"webIsolated=foo\"\n" //
        + "    },\n" //
        + "    \"session_id\": \"ca98fe03-1248-448f-bbdf-59f97dba5a0e\"\n" //
        + "  },\n" //
        + "  \"client_id\": null\n" + "}").getBytes(StandardCharsets.UTF_8));

    Map<String, String> attributes = Maps.newHashMap(ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "telemetry").put(Attribute.DOCUMENT_TYPE, "crash")
        .put(Attribute.APP_UPDATE_CHANNEL, "nightly").put(Attribute.APP_VERSION, "68.0").build());

    assertThrows(MessageShouldBeDroppedException.class,
        () -> MessageScrubber.scrub(attributes, ping));

    attributes.put(Attribute.APP_UPDATE_CHANNEL, "beta");
    attributes.put(Attribute.APP_VERSION, "68");
    assertThrows(MessageShouldBeDroppedException.class,
        () -> MessageScrubber.scrub(attributes, ping));

    attributes.put(Attribute.APP_VERSION, "69");
    MessageScrubber.scrub(attributes, ping);
  }

  @Test
  public void testShouldScrubBhrBug1562011() throws Exception {
    ObjectNode ping = Json.readObjectNode(("{\n" //
        + "  \"payload\": {\n" //
        + "    \"hangs\": [\n" //
        + "      {\"remoteType\": \"webIsolated=foo\"},\n" //
        + "      {\"remoteType\": \"web\"}\n" //
        + "    ],\n" //
        + "    \"session_id\": \"ca98fe03-1248-448f-bbdf-59f97dba5a0e\"\n" //
        + "  },\n" //
        + "  \"client_id\": null\n" + "}").getBytes(StandardCharsets.UTF_8));

    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "telemetry").put(Attribute.DOCUMENT_TYPE, "bhr")
        .put(Attribute.APP_UPDATE_CHANNEL, "nightly").put(Attribute.APP_VERSION, "68.0").build();

    assertThrows(MessageShouldBeDroppedException.class,
        () -> MessageScrubber.scrub(attributes, ping));
  }

  @Test
  public void testShouldScrubBug1697602() throws Exception {
    final Map<String, String> attributes1 = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "telemetry")
        .put(Attribute.DOCUMENT_TYPE, "account-ecosystem").build();
    assertThrows(MessageShouldBeDroppedException.class,
        () -> MessageScrubber.scrub(attributes1, Json.createObjectNode()));
    final Map<String, String> attributes2 = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "firefox-accounts")
        .put(Attribute.DOCUMENT_TYPE, "account-ecosystem").build();
    assertThrows(MessageShouldBeDroppedException.class,
        () -> MessageScrubber.scrub(attributes2, Json.createObjectNode()));
  }

  @Test
  public void testBug1602844Affected() throws Exception {
    Map<String, String> baseAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, ParseUri.TELEMETRY)
        .put(Attribute.DOCUMENT_TYPE, "focus-event").put(Attribute.APP_NAME, "Lockbox").build();
    assertFalse(MessageScrubber.bug1602844Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "1.7.1").build()));
    assertTrue(MessageScrubber.bug1602844Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "1.7.0").build()));
    assertTrue(MessageScrubber.bug1602844Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "1.6.1").build()));
    assertTrue(MessageScrubber.bug1602844Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "1.3").build()));
    assertTrue(MessageScrubber.bug1602844Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "1.1.1").build()));
  }

  @Test
  public void testBug1162183Affected() throws Exception {
    Map<String, String> baseAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, ParseUri.TELEMETRY)
        .put(Attribute.DOCUMENT_TYPE, "saved-session").build();
    assertFalse(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "60.6.1").build()));
    assertFalse(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "42.1.0").build()));
    assertTrue(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "39.0.1").build()));
    assertTrue(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "40.0a2").build()));
    assertTrue(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "41.0").build()));
    assertTrue(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "41.0.2").build()));

    baseAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, ParseUri.TELEMETRY)
        .put(Attribute.DOCUMENT_TYPE, "first-shutdown").build();
    assertFalse(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "60.6.1").build()));
    assertTrue(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "41.0.2").build()));

    baseAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, ParseUri.TELEMETRY).put(Attribute.DOCUMENT_TYPE, "main")
        .build();
    assertFalse(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "60.6.1").build()));
    assertTrue(MessageScrubber.bug1162183Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "41.0.2").build()));
  }

  @Test
  public void testBug1642386Affected() throws Exception {
    Map<String, String> baseAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, ParseUri.TELEMETRY).put(Attribute.DOCUMENT_TYPE, "sync")
        .build();

    assertFalse(MessageScrubber.bug1642386Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "60.6.1").build()));
    assertFalse(MessageScrubber.bug1642386Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "28.1").build()));
    assertTrue(MessageScrubber.bug1642386Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "27.1").build()));
    assertFalse(MessageScrubber.bug1642386Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "100.0").build()));
    assertTrue(MessageScrubber.bug1642386Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "25.0.1").build()));
    assertTrue(MessageScrubber.bug1642386Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "26.1").build()));
    assertTrue(MessageScrubber.bug1642386Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "14.1").build()));
    assertTrue(MessageScrubber.bug1642386Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "8.0.2").build()));
    assertTrue(MessageScrubber.bug1642386Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.APP_VERSION, "10.0.2").build()));
  }

  @Test
  public void testBug1712850Affected() {
    Map<String, String> baseAttributes = ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE,
        "contextual-services");

    assertTrue(MessageScrubber.bug1712850Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.DOCUMENT_TYPE, "quicksuggest-impression").build()));
    assertFalse(MessageScrubber.bug1712850Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.DOCUMENT_TYPE, "quicksuggest-click").build()));
    assertFalse(MessageScrubber.bug1712850Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.DOCUMENT_TYPE, "topsites-impression").build()));
    assertFalse(MessageScrubber.bug1712850Affected(ImmutableMap.<String, String>builder()
        .putAll(baseAttributes).put(Attribute.DOCUMENT_TYPE, "topsites-click").build()));
  }

  @Test
  public void testRedactForBug1602844() throws Exception {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, ParseUri.TELEMETRY)
        .put(Attribute.DOCUMENT_TYPE, "focus-event").put(Attribute.APP_NAME, "Lockbox")
        .put(Attribute.APP_VERSION, "1.7.0").build();
    ObjectNode json = Json.readObjectNode(("{\n" //
        + "  \"arch\": \"arm64\",\n" //
        + "  \"events\": [\n" //
        + "    [\n" //
        + "      224264,\n" //
        + "      \"action\",\n" //
        + "      \"background\",\n" //
        + "      \"app\",\n" //
        + "      null,\n" //
        + "      {\n" //
        + "        \"fxauid\": \"should-be-redacted\"\n" //
        + "      }\n" //
        + "    ],\n" //
        + "    [\n" //
        + "      49,\n" //
        + "      \"action\",\n" //
        + "      \"startup\",\n" //
        + "      \"app\"\n" //
        + "    ]\n" //
        + "  ],\n" //
        + "  \"tz\": 0" //
        + "  },\n" //
        + "}").getBytes(StandardCharsets.UTF_8));

    assertFalse(json.path("events").path(0).path(5).path("fxauid").isNull());
    MessageScrubber.scrub(attributes, json);

    assertTrue(json.path("events").path(0).path(5).path("fxauid").isNull());
    assertFalse(json.path("events").path(0).path(5).path("fxauid").isMissingNode());
    assertEquals("app", json.path("events").path(0).path(3).textValue());
    assertEquals("0", json.path("tz").asText());
  }

  @Test
  public void testRedactForBug1162183() throws Exception {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, ParseUri.TELEMETRY)
        .put(Attribute.DOCUMENT_TYPE, "first-shutdown").put(Attribute.APP_VERSION, "41.0").build();
    ObjectNode json = Json.readObjectNode(("{\n" //
        + "  \"payload\": {\n" //
        + "    \"slowSQL\": {\n" //
        + "      \"mainThread\": {\n" //
        + "         \"SELECT * FROM foo\": [1, 200]\n" //
        + "      }," //
        + "      \"otherThreads\": {}\n" //
        + "    },\n" //
        + "    \"session_id\": \"ca98fe03-1248-448f-bbdf-59f97dba5a0e\"\n" //
        + "  },\n" //
        + "  \"client_id\": null\n" + "}").getBytes(StandardCharsets.UTF_8));

    assertFalse(json.path("payload").path("slowSQL").isNull());
    assertTrue(json.path("payload").has("slowSQL"));
    MessageScrubber.scrub(attributes, json);
    assertFalse(json.path("payload").has("slowSQL"));
    assertTrue(json.path("payload").path("slowSQL").isMissingNode());
  }

  @Test
  public void testRedactForBug1642386() throws Exception {
    ObjectNode json = Json.readObjectNode(("{\n" //
        + "  \"payload\": {\n" //
        + "    \"syncs\": [{\n" //
        + "      \"engines\": [" //
        + "        {\n" //
        + "          \"outgoing\": {\n" //
        + "            \"failed\": 10,\n" //
        + "            \"sent\": 23\n" //
        + "          }\n" //
        + "        },{\n" //
        + "          \"outgoing\": {\n" //
        + "            \"failed\": 2,\n" //
        + "            \"sent\": 0\n" //
        + "          }\n" //
        + "        }\n" //
        + "      ]}, {\n" //
        + "      \"engines\": [" //
        + "        {\n" //
        + "          \"outgoing\": {\n" //
        + "            \"failed\": 1,\n" //
        + "            \"sent\": 3\n" //
        + "          }\n" //
        + "        },{\n" //
        + "          \"outgoing\": {\n" //
        + "            \"failed\": 28,\n" //
        + "            \"sent\": 70\n" //
        + "          }\n" //
        + "        }\n" //
        + "      ]}\n" //
        + "    ]\n" //
        + "  },\n" //
        + "  \"client_id\": null\n" + "}").getBytes(StandardCharsets.UTF_8));
    final Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, ParseUri.TELEMETRY).put(Attribute.DOCUMENT_TYPE, "sync")
        .put(Attribute.APP_VERSION, "25.1").build();

    assertFalse(json.path("payload").path("syncs").path(0).path("engines").path(0).path("outgoing")
        .isNull());
    assertFalse(json.path("payload").path("syncs").path(0).path("engines").path(1).path("outgoing")
        .isNull());
    assertFalse(json.path("payload").path("syncs").path(1).path("engines").path(0).path("outgoing")
        .isNull());
    assertFalse(json.path("payload").path("syncs").path(1).path("engines").path(1).path("outgoing")
        .isNull());
    MessageScrubber.scrub(attributes, json);
    assertTrue(json.path("payload").path("syncs").path(0).path("engines").path(0).path("outgoing")
        .isMissingNode());
    assertTrue(json.path("payload").path("syncs").path(0).path("engines").path(1).path("outgoing")
        .isMissingNode());
    assertFalse(json.path("payload").path("syncs").path(0).path("engines").path(0).has("outgoing"));
    assertFalse(json.path("payload").path("syncs").path(0).path("engines").path(1).has("outgoing"));
    assertFalse(json.path("payload").path("syncs").path(1).path("engines").path(0).has("outgoing"));
    assertFalse(json.path("payload").path("syncs").path(1).path("engines").path(1).has("outgoing"));
  }

  @Test
  public void testRedactForBug1712850() throws Exception {
    ObjectNode json = Json.readObjectNode(("{\n" //
        + "  \"search_query\": \"abc\",\n" //
        + "  \"matched_keywords\": \"abc\"\n" //
        + "}").getBytes(StandardCharsets.UTF_8));

    final Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "contextual-services")
        .put(Attribute.DOCUMENT_TYPE, "quicksuggest-impression").build();

    assertTrue(json.hasNonNull("search_query"));
    assertTrue(json.hasNonNull("matched_keywords"));
    MessageScrubber.scrub(attributes, json);
    assertEquals("", json.path("search_query").asText());
    assertEquals("", json.path("matched_keywords").asText());
  }

  @Test
  public void testShouldScrubClientIdBug1489560() throws Exception {
    ObjectNode pingToBeScrubbed = Json.readObjectNode(("{\n" //
        + "  \"client_info\": {\n" //
        + "    \"client_id\": \"c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0\"" //
        + "  }}").getBytes(StandardCharsets.UTF_8));

    Map<String, String> attributes = Maps.newHashMap(
        ImmutableMap.<String, String>builder().put(Attribute.DOCUMENT_NAMESPACE, "glean").build());

    assertThrows(AffectedByBugException.class,
        () -> MessageScrubber.scrub(attributes, pingToBeScrubbed));

    ObjectNode validPing = Json.readObjectNode(("{\n" //
        + "  \"client_info\": {\n" //
        + "    \"client_id\": null" //
        + "  }}").getBytes(StandardCharsets.UTF_8));

    MessageScrubber.scrub(attributes, validPing);
  }

  @Test
  public void testBug1489560MalformedPayload() {
    // https://bugzilla.mozilla.org/show_bug.cgi?id=1614428
    // clients with client_id c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0 send malformed client_info

    String schemasLocation = "schemas.tar.gz";

    // payloads here are:
    // {"client_info":{"client_id":"c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0"}}
    // {"client_info":{"client_id":"f0ffeec0-ffee-c0ff-eec0-ffeec0ffeecc"}}
    final List<String> input = Arrays.asList("{\"attributeMap\": {" //
        + "\"document_namespace\": \"glean\"" //
        + ",\"document_type\": \"glean\"" //
        + ",\"document_version\": \"1\"" //
        + "},\"payload\": \"eyJjbGllbnRfaW5mbyI6eyJjbGllbnRfaWQiOiJjMGZmZWVjMC"
        + "1mZmVlLWMwZmYtZWVjMC1mZmVlYzBmZmVlYzAifX0=\"}",
        "{\"attributeMap\": {" //
            + "\"document_namespace\": \"glean\"" //
            + ",\"document_type\": \"glean\"" //
            + ",\"document_version\": \"1\"" //
            + "},\"payload\": \"eyJjbGllbnRfaW5mbyI6eyJjbGllbnRfaWQiOiJmMGZmZWVjMC"
            + "1mZmVlLWMwZmYtZWVjMC1mZmVlYzBmZmVlY2MifX0=\n\"}");

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.json.decode()).apply(ParsePayload.of(schemasLocation));

    PCollection<String> exceptions = result.failures().apply(MapElements
        .into(TypeDescriptors.strings()).via(message -> message.getAttribute("exception_class")));

    PAssert.that(result.output()).empty();

    final List<String> expectedError = Arrays.asList(
        "com.mozilla.telemetry.decoder.MessageScrubber$AffectedByBugException",
        "org.everit.json.schema.ValidationException");

    // If we get a ValidationException here, it means we successfully extracted version from
    // the payload and found a valid schema; we expect the payload to not validate.
    PAssert.that(exceptions).containsInAnyOrder(expectedError);

    pipeline.run();
  }

  @Test
  public void testBug1626020Affected() throws Exception {
    ObjectNode pingToBeScrubbed = Json.readObjectNode(("{\n" //
        + "  \"client_info\": {\n" //
        + "    \"client_id\": null" //
        + "  }}").getBytes(StandardCharsets.UTF_8));

    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "default-browser-agent")
        .put(Attribute.DOCUMENT_TYPE, "1").build();

    assertThrows(AffectedByBugException.class,
        () -> MessageScrubber.scrub(attributes, pingToBeScrubbed));
  }

  @Test
  public void testUnwantedDataBug1631849PioneerStudy() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "telemetry") //
        .put(Attribute.DOCUMENT_TYPE, "pioneer-study").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1633525FrecencyUpdate() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "telemetry")
        .put(Attribute.DOCUMENT_TYPE, "frecency-update").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedDataBug1656910SavedSession() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "telemetry")
        .put(Attribute.DOCUMENT_TYPE, "saved-session").build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(attributes, Json.createObjectNode()));
  }

  @Test
  public void testUnwantedData1585144() {
    MessageScrubber.scrubByUri(null);
    MessageScrubber.scrubByUri("/foo/bar");
    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrubByUri("/submit/sslreports"));
    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrubByUri("/submit/sslreports/"));
  }

  @Test
  public void testUnwantedData1684980GleanUserAgent() {
    Map<String, String> mozAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "firefox-desktop") //
        .put(Attribute.USER_AGENT,
            "Mozilla/5.0 (Windows NT 10.0; rv:78.0) Gecko/20100101 Firefox/78.0")
        .build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(mozAttributes, Json.createObjectNode()));

    // empty agent strings are also scrubbed
    Map<String, String> emptyAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "firefox-desktop") //
        .put(Attribute.USER_AGENT, "") //
        .build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(emptyAttributes, Json.createObjectNode()));

    // null agent strings...
    Map<String, String> nullAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "firefox-desktop") //
        .build();

    assertThrows(UnwantedDataException.class,
        () -> MessageScrubber.scrub(nullAttributes, Json.createObjectNode()));

    Map<String, String> gleanAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "firefox-desktop") //
        .put(Attribute.USER_AGENT, "Glean/33.9.1 (Rust on Windows)") //
        .build();

    MessageScrubber.scrub(gleanAttributes, Json.createObjectNode());
  }

  @Test
  public void testScrubValidDocument() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "namespace") //
        .put(Attribute.DOCUMENT_TYPE, "type") //
        .put(Attribute.APP_NAME, "name") //
        .put(Attribute.APP_VERSION, "version") //
        .put(Attribute.APP_UPDATE_CHANNEL, "channel") //
        .put(Attribute.APP_BUILD_ID, "build_id") //
        // USER_AGENT may be null
        .build();
    MessageScrubber.scrub(attributes, Json.createObjectNode());
  }
}
