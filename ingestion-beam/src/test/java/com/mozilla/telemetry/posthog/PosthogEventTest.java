package com.mozilla.telemetry.posthog;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class PosthogEventTest {

  @Test
  public void testToJson() throws JsonProcessingException {
    PosthogEvent event = PosthogEvent.builder().setUserId("test").setSampleId(1)
        .setEventType("bookmark.event").setPlatform("firefox-desktop").setLanguage("en")
        .setTime(1738620582500L).build();

    Map<String, String> experiments = new HashMap<>();
    experiments.put("experiment1", null);
    experiments.put("experiment2", "branch_b");
    PosthogEvent eventWithExtras = PosthogEvent.builder().setUserId("test").setSampleId(1)
        .setEventType("bookmark.event").setPlatform("firefox-desktop").setTime(1738620582500L)
        .setEventExtras("{" + "\"metadata1\":\"extra\"," + "\"metadata2\":\"more_extra\","
            + "\"intVal\":\"42\"," + "\"floatVal\":\"3.14\"," + "\"boolVal\":\"true\","
            + "\"nullVal\":null,"
            + "\"nested\":\"{\\\"subkey\\\":\\\"subval\\\",\\\"subnum\\\":7}\"" + "}")
        .setExperiments(experiments).build();

    String expectedJsonEvent = "{" + "\"event\":\"bookmark.event\","
        + "\"properties\":{\"distinct_id\":\"test\",\"sample_id\":1,"
        + "\"platform\":\"firefox-desktop\",\"language\":\"en\"}}";
    String expectedJsonEventWithExtras = "{" + "\"event\":\"bookmark.event\","
        + "\"properties\":{\"distinct_id\":\"test\",\"sample_id\":1,"
        + "\"event_extras_metadata1\":\"extra\"," + "\"event_extras_metadata2\":\"more_extra\","
        + "\"event_extras_intVal\":\"42\"," + "\"event_extras_floatVal\":\"3.14\","
        + "\"event_extras_boolVal\":\"true\"," + "\"event_extras_nullVal\":null,"
        + "\"event_extras_nested\":\"{\\\"subkey\\\":\\\"subval\\\",\\\"subnum\\\":7}\","
        + "\"platform\":\"firefox-desktop\","
        + "\"experiments\":{\"experiment2\":\"branch_b\",\"experiment1\":null}}}";

    Assert.assertEquals(expectedJsonEvent, event.toJson().toString());
    System.out.println(eventWithExtras.toJson().toString());
    System.out.println(expectedJsonEventWithExtras);
    Assert.assertEquals(expectedJsonEventWithExtras, eventWithExtras.toJson().toString());
  }

  @Test
  public void testToJsonWithNull() throws JsonProcessingException {
    PosthogEvent eventWithNullExtras = PosthogEvent.builder().setUserId("test").setSampleId(1)
        .setEventType("bookmark.event").setPlatform("firefox-desktop").setTime(1738620582500L)
        .setEventExtras("null").build();

    String expectedJsonEvent = "{" + "\"event\":\"bookmark.event\","
        + "\"properties\":{\"distinct_id\":\"test\",\"sample_id\":1,"
        + "\"platform\":\"firefox-desktop\"}}";

    Assert.assertEquals(expectedJsonEvent, eventWithNullExtras.toJson().toString());
  }
}
