package com.mozilla.telemetry.decoder.ipprivacy;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class HashClientInfoTest {

  private static final String ID_HASH_KEY_PATH = "src/test/resources/ipPrivacy/client_id.key";
  private static final String IP_HASH_KEY_PATH = "src/test/resources/ipPrivacy/client_ip.key";

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testHashKeysNotMixed() throws IOException, HashClientInfo.KeyLengthMismatchException {
    HashClientInfo hashClientInfo = HashClientInfo.of(ID_HASH_KEY_PATH, IP_HASH_KEY_PATH);

    byte[] idHashKey = hashClientInfo.getClientIdHashKey();
    byte[] ipHashKey = hashClientInfo.getClientIpHashKey();

    Assert.assertFalse(Arrays.equals(idHashKey, ipHashKey));
    Assert.assertArrayEquals(idHashKey, hashClientInfo.readBytes(ID_HASH_KEY_PATH));
    Assert.assertArrayEquals(ipHashKey, hashClientInfo.readBytes(IP_HASH_KEY_PATH));
  }

  @Test
  public void testOutputIsHashed() {
    String clientId = "client_id";
    String clientIp = "client_ip";

    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.CLIENT_ID, clientId).put(Attribute.CLIENT_IP, clientIp).build();
    PubsubMessage input = new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributes);

    PCollection<PubsubMessage> output = pipeline.apply(Create.of(input))
        .apply(HashClientInfo.of(ID_HASH_KEY_PATH, IP_HASH_KEY_PATH));

    PAssert.that(output).satisfies((SerializableFunction<Iterable<PubsubMessage>, Void>) input1 -> {
      for (PubsubMessage message : input1) {
        Assert.assertNotEquals(message.getAttribute(Attribute.CLIENT_ID), clientId);
        Assert.assertNotEquals(message.getAttribute(Attribute.CLIENT_IP), clientIp);
        Assert.assertTrue(HashClientInfo.isHashed(message.getAttribute(Attribute.CLIENT_ID)));
        Assert.assertTrue(HashClientInfo.isHashed(message.getAttribute(Attribute.CLIENT_IP)));
      }
      return null;
    });

    pipeline.run();
  }
}
