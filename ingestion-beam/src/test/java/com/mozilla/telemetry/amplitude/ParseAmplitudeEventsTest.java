package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ParseAmplitudeEventsTest {

  private static final String EVENTS_ALLOW_LIST = "src/test/resources/amplitude/eventsAllowlist.csv";

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testAllowedEventsLoadAndFilter() throws IOException {
    ParseAmplitudeEvents parseAmplitudeEvents;
    parseAmplitudeEvents = ParseAmplitudeEvents.of(EVENTS_ALLOW_LIST);
    pipeline.run();

    ArrayList<String[]> expectedAllowedEvents = new ArrayList<>();
    expectedAllowedEvents
        .add(new String[] { "firefox-desktop", "events", "accessibility", "dynamic_text" });
    expectedAllowedEvents
        .add(new String[] { "org-mozilla-fenix", "events", "nimbus_events", "enroll_failed" });
    expectedAllowedEvents
        .add(new String[] { "firefox-desktop", "quick-suggest", "top_site", "contile_click" });
    expectedAllowedEvents.add(new String[] { "firefox-desktop", "events", "bookmark", "*" });
    List<String[]> allowedEvents = parseAmplitudeEvents.readAllowedEventsFromFile();
    assert expectedAllowedEvents.size() == allowedEvents.size();

    for (int i = 0; i < expectedAllowedEvents.size(); i++) {
      assert Arrays.equals(expectedAllowedEvents.get(i), allowedEvents.get(i));
    }
  }

  @Test
  public void testExtractEvents() throws IOException {
    final ObjectNode payload = Json.createObjectNode();

    ObjectNode eventObject = Json.createObjectNode();
    eventObject.put("category", "top_site");
    eventObject.put("name", "contile_click");
    eventObject.put("timestamp", "0");
    payload.putArray("events").add(eventObject);

    ObjectNode expectedEventObject = Json.createObjectNode();
    expectedEventObject.put("event_type", "top_site.contile_click");
    String nullValue = null;
    expectedEventObject.put("event_extras", nullValue);
    expectedEventObject.put("timestamp", "0");

    ArrayList<ObjectNode> expect = new ArrayList<ObjectNode>();
    expect.add(expectedEventObject);

    ParseAmplitudeEvents parseAmplitudeEvents = ParseAmplitudeEvents.of(EVENTS_ALLOW_LIST);
    parseAmplitudeEvents.readAllowedEventsFromFile();

    ArrayList<ObjectNode> actual = parseAmplitudeEvents.extractEvents(payload, "firefox-desktop",
        "quick-suggest");
    if (!expect.equals(actual)) {
      System.err.println(Json.asString(actual));
      System.err.println(Json.asString(expect));
    }
    assert expect.equals(actual);
  }

  @Test
  public void testParsedAmplitudeEvents() {
    final Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "events",
        Attribute.CLIENT_ID, "xxx", Attribute.DOCUMENT_NAMESPACE, "firefox-desktop",
        Attribute.USER_AGENT_OS, "Windows", Attribute.SUBMISSION_TIMESTAMP, "2022-03-15T16:42:38Z");

    List<PubsubMessage> input = Stream.of("dynamic_text", "non_existing").map(eventName -> {
      final ObjectNode payload = Json.createObjectNode();
      payload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
      payload.put(Attribute.VERSION, "87.0");
      final ArrayNode events = payload.putArray("events");

      ObjectNode eventObject = Json.createObjectNode();
      eventObject.put("category", "accessibility");
      eventObject.put("name", eventName);
      eventObject.put(Attribute.TIMESTAMP, "0");

      ObjectNode eventExtra = Json.createObjectNode();
      eventExtra.put(Attribute.GLEAN_TIMESTAMP, "1738782714952");
      eventObject.put("extra", eventExtra);
      events.add(eventObject);

      return payload;
    }).map(payload -> new PubsubMessage(Json.asBytes(payload), attributes))
        .collect(Collectors.toList());

    Result<PCollection<AmplitudeEvent>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseAmplitudeEvents.of(EVENTS_ALLOW_LIST));

    PAssert.that(result.output()).satisfies(amplitudeEvents -> {
      List<AmplitudeEvent> payloads = new ArrayList<>();
      amplitudeEvents.forEach(payloads::add);

      Assert.assertEquals("1 events in output", 1, payloads.size());

      Assert.assertTrue("amplitude event type is accessibility.dynamic_text",
          payloads.get(0).getEventType().equals("accessibility.dynamic_text"));
      Assert.assertTrue("amplitude event user ID is xxx",
          payloads.get(0).getUserId().equals("xxx"));
      Assert.assertTrue("amplitude event app version is null",
          payloads.get(0).getAppVersion() == null);
      Assert.assertTrue("amplitude event timestamp is ",
          payloads.get(0).getTime() == 1738782714952L);

      return null;
    });

    pipeline.run();
  }

  @Test
  public void testParsedAmplitudeEventsEventTimestamps() {
    final Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "events",
        Attribute.CLIENT_ID, "xxx", Attribute.DOCUMENT_NAMESPACE, "firefox-desktop",
        Attribute.USER_AGENT_OS, "Windows", Attribute.SUBMISSION_TIMESTAMP, "2022-03-15T16:42:38Z");

    final ObjectNode payload1 = Json.createObjectNode();
    payload1.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    payload1.put(Attribute.VERSION, "87.0");

    ObjectNode pingInfo = Json.createObjectNode();
    pingInfo.put(Attribute.PARSED_START_TIME, "2022-03-15T16:42:38Z");
    payload1.put(Attribute.PING_INFO, pingInfo);
    final ArrayNode events1 = payload1.putArray("events");

    String nullValue = null;
    ObjectNode eventObject1 = Json.createObjectNode();
    eventObject1.put("category", "bookmark");
    eventObject1.put("name", nullValue);
    eventObject1.put(Attribute.TIMESTAMP, "10");

    ObjectNode eventObject2 = Json.createObjectNode();
    eventObject2.put("category", "bookmark");
    eventObject2.put("name", "add");
    eventObject2.put(Attribute.TIMESTAMP, "10");

    events1.add(eventObject1);
    events1.add(eventObject2);

    final ObjectNode payload2 = Json.createObjectNode();
    payload2.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    payload2.put(Attribute.VERSION, "87.0");

    final ArrayNode events2 = payload2.putArray("events");

    ObjectNode eventObject3 = Json.createObjectNode();
    eventObject3.put("category", "bookmark");
    eventObject3.put("name", "test");
    eventObject3.put(Attribute.TIMESTAMP, "10");

    // gets filtered due to non-matching category
    ObjectNode eventObject4 = Json.createObjectNode();
    eventObject4.put("category", "test");
    eventObject4.put("name", "foo");
    eventObject4.put(Attribute.TIMESTAMP, "1");

    events2.add(eventObject3);
    events2.add(eventObject4);

    // gets filtered due to non-matching doctype and namespace
    final Map<String, String> attributes2 = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "test",
        Attribute.CLIENT_ID, "xxx", Attribute.DOCUMENT_NAMESPACE, "test", Attribute.USER_AGENT_OS,
        "Windows", Attribute.SUBMISSION_TIMESTAMP, "2022-03-15T16:42:38Z");

    final ObjectNode payload3 = Json.createObjectNode();
    final ArrayNode events3 = payload3.putArray("events");

    ObjectNode eventObject5 = Json.createObjectNode();
    eventObject5.put("category", "bookmark");
    eventObject5.put("name", "xxxxx");
    eventObject5.put(Attribute.TIMESTAMP, "0");

    events3.add(eventObject5);

    List<PubsubMessage> input = ImmutableList.of(
        new PubsubMessage(Json.asBytes(payload1), attributes),
        new PubsubMessage(Json.asBytes(payload2), attributes),
        new PubsubMessage(Json.asBytes(payload3), attributes2));

    Result<PCollection<AmplitudeEvent>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseAmplitudeEvents.of(EVENTS_ALLOW_LIST));

    PAssert.that(result.output()).satisfies(amplitudeEvents -> {
      List<AmplitudeEvent> payloads = new ArrayList<>();
      amplitudeEvents.forEach(payloads::add);

      Assert.assertEquals("3 events in output", 3, payloads.size());

      for (AmplitudeEvent event : amplitudeEvents) {
        if (event.getEventType().equals("bookmark.add")) {
          Assert.assertTrue("amplitude event timestamp is 1647362558010l",
              event.getTime() == 1647362558010L);
        } else if (event.getEventType().equals("bookmark")) {
          Assert.assertTrue("amplitude event timestamp is 1647362558010l",
              event.getTime() == 1647362558010L);
        } else if (event.getEventType().equals("bookmark.test")) {
          Assert.assertTrue("amplitude event timestamp is larger 0", payloads.get(2).getTime() > 0);
        } else {
          System.err.println(event.getEventType());
          Assert.assertTrue("amplitude event with unknown type", false);
        }
      }

      return null;
    });

    pipeline.run();
  }

  @Test
  public void testParsedAmplitudeEventsWithMissingClientId() {
    final Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "events",
        Attribute.DOCUMENT_NAMESPACE, "firefox-desktop", Attribute.USER_AGENT_OS, "Windows",
        Attribute.SUBMISSION_TIMESTAMP, "2022-03-15T16:42:38Z");

    final ObjectNode payload = Json.createObjectNode();
    payload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    payload.put(Attribute.VERSION, "87.0");
    final ArrayNode events = payload.putArray("events");

    ObjectNode eventObject = Json.createObjectNode();
    eventObject.put("category", "accessibility");
    eventObject.put("name", "test");
    eventObject.put(Attribute.TIMESTAMP, "0");

    ObjectNode eventExtra = Json.createObjectNode();
    eventExtra.put(Attribute.GLEAN_TIMESTAMP, "1738782714952");
    eventObject.put("extra", eventExtra);
    events.add(eventObject);

    List<PubsubMessage> input = ImmutableList
        .of(new PubsubMessage(Json.asBytes(payload), attributes));

    Result<PCollection<AmplitudeEvent>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseAmplitudeEvents.of(EVENTS_ALLOW_LIST));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterators.size(messages.iterator()));
      return null;
    });

    pipeline.run();
  }
}
