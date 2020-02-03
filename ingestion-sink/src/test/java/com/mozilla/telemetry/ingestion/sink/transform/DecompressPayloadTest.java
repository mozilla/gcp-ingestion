package com.mozilla.telemetry.ingestion.sink.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.util.Base64;
import java.util.Map;
import org.junit.Test;

public class DecompressPayloadTest {

  @Test
  public void canNoneDecompressNullMessage() {
    DecompressPayload.NONE.apply(null);
  }

  @Test(expected = NullPointerException.class)
  public void failsGzipDecompressNullMessage() {
    DecompressPayload.GZIP.apply(null);
  }

  @Test
  public void canPassThroughNotCompressed() {
    for (DecompressPayload decompress : DecompressPayload.values()) {
      PubsubMessage message = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("test"))
          .build();
      assertEquals(message, decompress.apply(message));
    }
  }

  @Test
  public void canHandleEmptyMessage() {
    for (DecompressPayload decompress : DecompressPayload.values()) {
      assertTrue(decompress.apply(PubsubMessage.newBuilder().build()).getData().isEmpty());
    }
  }

  @Test
  public void canGzipDecompress() {
    final ByteString data = ByteString
        .copyFrom(Base64.getDecoder().decode("H4sIAM1ekFsAAytJLS4BAAx+f9gEAAAA"));
    final Map<String, String> attributes = ImmutableMap.of("meta", "data");
    final PubsubMessage result = DecompressPayload.GZIP
        .apply(PubsubMessage.newBuilder().putAllAttributes(attributes).setData(data).build());
    assertEquals("test", result.getData().toStringUtf8());
    assertEquals(attributes, result.getAttributesMap());
  }
}
