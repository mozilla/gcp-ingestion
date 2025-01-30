package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.mozilla.telemetry.util.Json;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import java.util.ArrayList;
import com.fasterxml.jackson.core.JsonProcessingException;

public class ParseAmplitudeEventsTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testExtractEvents() throws JsonProcessingException {
    final ObjectNode payload = Json.createObjectNode();

    ObjectNode eventObject = Json.createObjectNode();
    eventObject.put("category", "top_site");
    eventObject.put("name", "contile_click");
    eventObject.put("timestamp", "0");
    payload.putArray("events").add(eventObject);

    ArrayList<ObjectNode> expect = new ArrayList<ObjectNode>();
    ObjectNode expectedEventObject = Json.createObjectNode();
    expectedEventObject.put("event_type", "top_site.contile_click");
    String nullValue = null;
    expectedEventObject.put("event_properties", nullValue);
    expect.add(expectedEventObject);
    ArrayList<ObjectNode> actual = ParseAmplitudeEvents.extractEvents(payload);
    if (!expect.equals(actual)) {
      System.err.println(Json.asString(actual));
      System.err.println(Json.asString(expect));
    }
    assert expect.equals(actual);
  }
}
