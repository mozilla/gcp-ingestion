package com.mozilla.telemetry.decoder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class MessageScrubberTest {

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
    assertTrue(MessageScrubber.shouldScrub(attributes, bug1567596AffectedJson));
    assertFalse(MessageScrubber.shouldScrub(new HashMap<>(), bug1567596AffectedJson));
    assertFalse(MessageScrubber.shouldScrub(attributes, Json.createObjectNode()));
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

    assertTrue(MessageScrubber.shouldScrub(attributes, ping));

    attributes.put(Attribute.APP_UPDATE_CHANNEL, "beta");
    attributes.put(Attribute.APP_VERSION, "68");
    assertTrue(MessageScrubber.shouldScrub(attributes, ping));

    attributes.put(Attribute.APP_VERSION, "69");
    assertFalse(MessageScrubber.shouldScrub(attributes, ping));
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

    assertTrue(MessageScrubber.shouldScrub(attributes, ping));
  }

}
