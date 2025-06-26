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
        .setEventExtras("{\"metadata1\":\"extra\",\"metadata2\":\"more_extra\"}")
        .setExperiments(experiments).build();

    String expectedJsonEvent = "{" + "\"event\":\"bookmark.event\","
        + "\"properties\":{\"distinct_id\":\"test\",\"sample_id\":1,"
        + "\"platform\":\"firefox-desktop\",\"language\":\"en\"}}";
    String expectedJsonEventWithExtras = "{" + "\"event\":\"bookmark.event\","
        + "\"properties\":{\"distinct_id\":\"test\",\"sample_id\":1,"
        + "\"event_extras\":{\"metadata1\":\"extra\",\"metadata2\":\"more_extra\"},"
        + "\"platform\":\"firefox-desktop\","
        + "\"experiments\":{\"experiment2\":\"branch_b\",\"experiment1\":null}}}";

    Assert.assertEquals(expectedJsonEvent, event.toJson().toString());
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
