package com.mozilla.telemetry;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.junit.Test;

public class PosthogPublisherTest {

  private static final String API_KEYS = "src/test/resources/posthog/apiKeys.csv";

  @Test
  public void testReadPosthogApiKeysFromFile() {
    Map<String, String> apiKeys = PosthogPublisher.readPosthogApiKeysFromFile(API_KEYS);
    assertEquals("xxxx", apiKeys.get("org-mozilla-ios-firefox"));
    assertEquals("yyyy", apiKeys.get("org-mozilla-firefox"));
  }
}
