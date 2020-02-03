package com.mozilla.telemetry.ingestion.sink.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.junit.Test;

public class CompressPayloadTest {

  private static final Base64.Encoder BASE64 = Base64.getEncoder();

  @Test
  public void canNoneCompressNullMessage() {
    CompressPayload.NONE.apply(null);
  }

  @Test(expected = NullPointerException.class)
  public void failsGzipCompressNullMessage() {
    CompressPayload.GZIP.apply(null);
  }

  @Test
  public void canNoneCompress() {
    PubsubMessage message = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("test"))
        .build();
    assertEquals(message, CompressPayload.NONE.apply(message));
  }

  @Test
  public void canGzipCompress() {
    final Map<String, String> attributes = ImmutableMap.of("meta", "data");
    for (String input : ImmutableList.of("", "test")) {
      final PubsubMessage message = PubsubMessage.newBuilder().putAllAttributes(attributes)
          .setData(ByteString.copyFromUtf8(input)).build();
      final PubsubMessage result = CompressPayload.GZIP.apply(message);
      assertNotEquals(BASE64.encodeToString(input.getBytes(StandardCharsets.UTF_8)),
          BASE64.encodeToString(result.getData().toByteArray()));
      assertEquals(attributes, result.getAttributesMap());
      assertEquals(input, DecompressPayload.GZIP.apply(result).getData().toStringUtf8());
    }
  }
}
