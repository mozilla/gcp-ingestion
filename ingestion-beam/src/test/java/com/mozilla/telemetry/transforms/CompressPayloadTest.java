package com.mozilla.telemetry.transforms;

import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.junit.Test;

public class CompressPayloadTest {

  @Test
  public void testGzipCompress() {
    String text = StringUtils.repeat("Lorem ipsum dolor sit amet ", 100);
    byte[] compressedBytes = CompressPayload.compress(text.getBytes(StandardCharsets.UTF_8),
        Compression.GZIP);
    assertThat(ArrayUtils.toObject(compressedBytes), Matchers.arrayWithSize(68));
  }

  @Test
  public void testMaxCompressedBytes() {
    String text = StringUtils.repeat("Lorem ipsum dolor sit amet ", 100);
    int expectedCompressedSize = 68;
    CompressPayload transform = CompressPayload.of(Compression.GZIP)
        .withMaxCompressedBytes(expectedCompressedSize - 1);
    PubsubMessage truncated = transform
        .compress(new PubsubMessage(text.getBytes(StandardCharsets.UTF_8), new HashMap<>()));
    assertThat(ArrayUtils.toObject(truncated.getPayload()), Matchers.arrayWithSize(50));
  }

}
