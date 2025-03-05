package com.mozilla.telemetry;

import java.util.Map;
import org.junit.Test;

public class AmplitudePublisherTest {

  private static final String API_KEYS = "src/test/resources/amplitude/apiKeys.csv";

  @Test
  public void testReadAmplitudeApiKeysFromFile() {
    Map<String, String> apiKeys = AmplitudePublisher.readAmplitudeApiKeysFromFile(API_KEYS);

    assert apiKeys.get("org-mozilla-ios-firefox").equals("xxxx");
    assert apiKeys.get("org-mozilla-firefox").equals("yyyy");
  }
}
