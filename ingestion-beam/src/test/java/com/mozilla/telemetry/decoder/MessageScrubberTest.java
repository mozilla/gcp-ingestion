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
import org.apache.beam.sdk.options.ValueProvider;
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

    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");

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
}
