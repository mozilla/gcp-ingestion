package com.mozilla.telemetry.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class GzipUtilTest {

  @Test
  public void testIsGzip() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"payload\":\"H4sIAM1ekFsAAytJLS4BAAx+f9gEAAAA\"}");

    input.stream().map(this::getPayload).map(GzipUtil::isGzip).forEach(System.out::println);
  }

  private byte[] getPayload(String input) {
    try {
      return Json.readPubsubMessage(input).getPayload();
    } catch (IOException e) {
      throw new RuntimeException("Could not convert to PubsubMessage");
    }
  }
}
