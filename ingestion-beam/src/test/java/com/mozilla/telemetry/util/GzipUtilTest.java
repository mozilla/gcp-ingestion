package com.mozilla.telemetry.util;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class GzipUtilTest {

  @Test
  public void testIsGzip() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        // base64-encoded gzipped bytes
        "{\"payload\":\"H4sIAM1ekFsAAytJLS4BAAx+f9gEAAAA\"}",
        // base64-encoded almost gzipped bytes
        "{\"payload\":\"iwgAzV6QWwADK0ktLgEADH5/2AQAAAA=\"}");

    List<Boolean> results = input.stream().map(this::getPayload).map(GzipUtil::isGzip)
        .collect(Collectors.toList());

    Assert.assertEquals(ImmutableList.of(false, true, false), results);
  }

  private byte[] getPayload(String input) {
    try {
      return Json.readPubsubMessage(input).getPayload();
    } catch (IOException e) {
      throw new RuntimeException("Could not convert to PubsubMessage: " + e.getMessage());
    }
  }
}
