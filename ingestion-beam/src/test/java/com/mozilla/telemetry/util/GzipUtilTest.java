package com.mozilla.telemetry.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Test;

public class GzipUtilTest {

  @Test
  public void testIsGzip2() {
    final Base64.Decoder decoder = Base64.getDecoder();

    Assert.assertFalse(GzipUtil.isGzip("{}".getBytes(StandardCharsets.UTF_8)));
    // base64-encoded gzipped bytes
    Assert.assertTrue(GzipUtil.isGzip(decoder.decode("H4sIAM1ekFsAAytJLS4BAAx+f9gEAAAA")));
    // base64-encoded almost gzipped bytes
    Assert.assertFalse(GzipUtil.isGzip(decoder.decode("iwgAzV6QWwADK0ktLgEADH5/2AQAAAA=")));
  }
}
