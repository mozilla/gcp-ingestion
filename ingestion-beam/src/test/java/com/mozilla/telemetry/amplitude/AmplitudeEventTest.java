package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class AmplitudeEventTest {

  @Test
  public void testToJson() throws JsonProcessingException {
    AmplitudeEvent event = AmplitudeEvent.builder().setUserId("test").setEventType("bookmark.event")
        .setPlatform("firefox-desktop").setTime(1738620582500L).build();

    Map<String, String> experiments = new HashMap<>();
    experiments.put("experiment1", null);
    experiments.put("experiment2", "branch_b");
    AmplitudeEvent eventWithExtras = AmplitudeEvent.builder().setUserId("test")
        .setEventType("bookmark.event").setPlatform("firefox-desktop").setTime(1738620582500L)
        .setEventExtras("{\"metadata1\":\"extra\",\"metadata2\":\"more_extra\"}")
        .setExperiments(experiments).build();

    String expectedJsonEvent = "{\"user_id\":\"test\",\"event_properties\":"
        + "{\"extra\":{}},\"event_type\":\"bookmark.event\",\"platform\":\"firefox-desktop\"}";
    String expectedJsonEventWithExtras = "{\"user_id\":\"test\",\"user_properties\":"
        + "{\"experiment2\":\"branch_b\",\"experiment1\":null},\"event_properties\":"
        + "{\"extra\":{\"metadata1\":\"extra\",\"metadata2\":\"more_extra\"}},"
        + "\"event_type\":\"bookmark.event\",\"platform\":\"firefox-desktop\"}";

    Assert.assertEquals(event.toJson().toString(), expectedJsonEvent);
    Assert.assertEquals(eventWithExtras.toJson().toString(), expectedJsonEventWithExtras);
  }

  @Test
  public void testToJsonWithNull() throws JsonProcessingException {
    AmplitudeEvent eventWithNullExtras = AmplitudeEvent.builder().setUserId("test")
        .setEventType("bookmark.event").setPlatform("firefox-desktop").setTime(1738620582500L)
        .setEventExtras("null").build();

    String expectedJsonEvent = "{\"user_id\":\"test\",\"event_properties\":"
        + "{\"extra\":{}},\"event_type\":\"bookmark.event\",\"platform\":\"firefox-desktop\"}";

    Assert.assertEquals(eventWithNullExtras.toJson().toString(), expectedJsonEvent);
  }
}
