package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import java.util.ArrayList;
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

  // @Test
  // public void testAllowedEventsLoadAndFilter() throws IOException {
  // ParseAmplitudeEvents parseAmplitudeEvents = ParseAmplitudeEvents.of(EVENTS_ALLOW_LIST);

  // pipeline.run();

  // List<String[]> allowedEvents = parseAmplitudeEvents.readAllowedEventsFromFile();
  // ArrayList<String[]> expectedAllowedEvents = new ArrayList<>();
  // expectedAllowedEvents.add(new String[] {"firefox_desktop", "events", "accessibility",
  // "dynamic_text"});
  // expectedAllowedEvents.add(new String[] {"org_mozilla_fenix", "events", "nimbus_events",
  // "enroll_failed"});
  // expectedAllowedEvents.add(new String[] {"firefox_desktop", "quick_suggest", "top_site",
  // "contile_click"});

  // assert expectedAllowedEvents.size() == allowedEvents.size();

  // for (int i = 0; i < expectedAllowedEvents.size(); i++) {
  // assert Arrays.equals(expectedAllowedEvents.get(i), allowedEvents.get(i));
  // }
  // }

  // @Test
  // public void testExtractEvents() throws JsonProcessingException, IOException {
  // final ObjectNode payload = Json.createObjectNode();

  // ObjectNode eventObject = Json.createObjectNode();
  // eventObject.put("category", "top_site");
  // eventObject.put("name", "contile_click");
  // eventObject.put("timestamp", "0");
  // payload.putArray("events").add(eventObject);

  // ArrayList<ObjectNode> expect = new ArrayList<ObjectNode>();
  // ObjectNode expectedEventObject = Json.createObjectNode();
  // expectedEventObject.put("event_type", "top_site.contile_click");
  // String nullValue = null;
  // expectedEventObject.put("event_extras", nullValue);
  // expect.add(expectedEventObject);

  // ParseAmplitudeEvents parseAmplitudeEvents = ParseAmplitudeEvents.of(EVENTS_ALLOW_LIST);
  // parseAmplitudeEvents.readAllowedEventsFromFile();

  // ArrayList<ObjectNode> actual = parseAmplitudeEvents.extractEvents(payload, "firefox_desktop",
  // "quick_suggest");
  // if (!expect.equals(actual)) {
  // System.err.println(Json.asString(actual));
  // System.err.println(Json.asString(expect));
  // }
  // assert expect.equals(actual);
  // }

  @Test
  public void testParsedAmplitudeEvents() {
    final Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "events",
        Attribute.CLIENT_ID, "xxx", Attribute.DOCUMENT_NAMESPACE, "firefox_desktop",
        Attribute.USER_AGENT_OS, "Windows", Attribute.SUBMISSION_TIMESTAMP, "2022-03-15T16:42:38Z");

    List<PubsubMessage> input = Stream.of("dynamic_text", "non_existing").map(eventName -> {
      final ObjectNode payload = Json.createObjectNode();
      payload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
      payload.put(Attribute.VERSION, "87.0");
      final ArrayNode events = payload.putArray("events");

      ObjectNode eventObject = Json.createObjectNode();
      eventObject.put("category", "accessibility");
      eventObject.put("name", eventName);
      eventObject.put("timestamp", "0");
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

      System.err.println(payloads.get(0).getEventType());

      Assert.assertTrue("amplitude event type is accessibility.dynamic_text",
          payloads.get(0).getEventType().equals("accessibility.dynamic_text"));
      Assert.assertTrue("amplitude event user ID is xxx",
          payloads.get(0).getUserId().equals("xxx"));
      Assert.assertTrue("amplitude event app version is null",
          payloads.get(0).getAppVersion() == null);

      return null;
    });

    pipeline.run();
  }
}
