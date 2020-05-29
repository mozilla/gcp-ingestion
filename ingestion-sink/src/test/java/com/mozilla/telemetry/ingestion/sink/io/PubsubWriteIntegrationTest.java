package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.SinglePubsubTopic;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class PubsubWriteIntegrationTest {

  @Rule
  public final SinglePubsubTopic pubsub = new SinglePubsubTopic();

  @Rule
  public final Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

  @Test
  public void canWriteToStaticDestination() {
    new Pubsub.Write(pubsub.getTopic(), ForkJoinPool.commonPool(), b -> b, m -> m)
        .apply(PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8))).build())
        .join();
    assertEquals(ImmutableList.of("test"), pubsub.pull(1, false).stream()
        .map(m -> m.getData().toStringUtf8()).collect(Collectors.toList()));
  }

  @Test
  public void canWriteToDynamicDestination() {
    new Pubsub.Write("${topic}", ForkJoinPool.commonPool(), b -> b, m -> m).apply(PubsubMessage
        .newBuilder().setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
        .putAttributes("topic", pubsub.getTopic()).build()).join();
    assertEquals(ImmutableList.of("test"), pubsub.pull(1, false).stream()
        .map(m -> m.getData().toStringUtf8()).collect(Collectors.toList()));
  }
}
