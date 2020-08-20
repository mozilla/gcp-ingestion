package com.mozilla.telemetry.aet;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.util.Json;
import org.junit.Test;

public class SanitizeJsonPayloadTest {

  @Test
  public void testSanitizeJsonNode() {
    String anonId = "eyJrbGciOiJFQ0RILUVTK0EyNTZLVyIsImVuYyI6IkEyNTZHQ00iLCJlc0EyNTZLVyIsImVuYyI6I";
    ObjectNode json = Json.asObjectNode(
        ImmutableMap.of("payload", ImmutableMap.of("myList", ImmutableList.of("foo", anonId))));
    ObjectNode expected = Json.asObjectNode(ImmutableMap.of("payload",
        ImmutableMap.of("myList", ImmutableList.of("foo", "eyJr<73 characters redacted>"))));
    SanitizeJsonPayload.sanitizeJsonNode(json);
    assertEquals(Json.asString(expected), Json.asString(json));
  }

}
