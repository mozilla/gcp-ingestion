package com.mozilla.telemetry.ingestion.sink.io;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.SinglePubsubTopic;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.Rule;
import org.junit.Test;

public class PubsubStreamingPullIntegrationTest {

  private static final PubsubMessage TEST_MESSAGE = PubsubMessage.newBuilder()
      .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8))).build();

  @Rule
  public final SinglePubsubTopic pubsub = new SinglePubsubTopic();

  @Rule
  public final LoggerContextRule logs = new LoggerContextRule("log4j2-test.yaml");

  @Test
  public void canReadOneMessage() {
    pubsub.publish(TEST_MESSAGE);

    AtomicReference<PubsubMessage> received = new AtomicReference<>();
    AtomicReference<Input> input = new AtomicReference<>();

    input.set(new Pubsub.StreamingPull(pubsub.getSubscription(),
        // handler
        message -> CompletableFuture.supplyAsync(() -> message) // create a future with message
            .thenAccept(received::set) // add message to received
            .thenRun(() -> input.get().stop()), // stop the subscriber
        // config
        builder -> pubsub.channelProvider
            .map(channelProvider -> builder.setChannelProvider(channelProvider)
                .setCredentialsProvider(pubsub.noCredentialsProvider))
            .orElse(builder),
        m -> m));

    input.get().run();

    assertEquals("test", new String(received.get().getData().toByteArray()));
  }

  @Test
  public void canRetryOnException() {

    String messageId = pubsub.publish(TEST_MESSAGE);

    List<PubsubMessage> received = new LinkedList<>();
    AtomicReference<Input> input = new AtomicReference<>();

    input.set(new Pubsub.StreamingPull(pubsub.getSubscription(),
        // handler
        message -> CompletableFuture.completedFuture(message) // create a future with message
            .thenAccept(received::add) // add message to received
            .thenRun(() -> {
              // throw an error to nack the message the first time
              if (received.size() == 1) {
                throw new RuntimeException("test");
              }
            }).thenRun(() -> input.get().stop()), // stop the subscriber
        // config
        builder -> pubsub.channelProvider
            .map(channelProvider -> builder.setChannelProvider(channelProvider)
                .setCredentialsProvider(pubsub.noCredentialsProvider))
            .orElse(builder),
        m -> m));

    input.get().run();

    assertEquals(messageId, received.get(0).getMessageId());
    assertEquals(messageId, received.get(1).getMessageId());
    assertEquals(2, received.size());
    assertThat(logs.getListAppender("STDOUT").getMessages(),
        containsInAnyOrder(containsString("java.lang.RuntimeException: test")));
  }
}
