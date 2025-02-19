package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Assert;
import org.junit.Test;

public class AmplitudeEventTest {

  @Test
  public void testToJson() throws JsonProcessingException {
    AmplitudeEvent event = AmplitudeEvent.builder().setUserId("test").setEventType("bookmark.event")
        .setPlatform("firefox-desktop").setTime(1738620582500L).build();

    AmplitudeEvent eventWithExtras = AmplitudeEvent.builder().setUserId("test")
        .setEventType("bookmark.event").setPlatform("firefox-desktop").setTime(1738620582500L)
        .setEventExtras("{\"metadata1\":\"extra\",\"metadata2\":\"more_extra\"}").build();

    String expectedJsonEvent = "{\"user_id\":\"test\",\"event_properties\":"
        + "{\"extras\":{}},\"event_type\":\"bookmark.event\",\"platform\":\"firefox-desktop\"}";
    String expectedJsonEventWithExtras = "{\"user_id\":\"test\",\"event_properties\":"
        + "{\"extras\":{\"metadata1\":\"extra\",\"metadata2\":\"more_extra\"}},"
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
        + "{\"extras\":{}},\"event_type\":\"bookmark.event\",\"platform\":\"firefox-desktop\"}";

    Assert.assertEquals(eventWithNullExtras.toJson().toString(), expectedJsonEvent);
  }
}
