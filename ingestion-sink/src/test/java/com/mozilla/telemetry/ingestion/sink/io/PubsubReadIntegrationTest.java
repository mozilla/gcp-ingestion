package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.TestWithSinglePubsubTopic;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class PubsubReadIntegrationTest extends TestWithSinglePubsubTopic {

  private static final PubsubMessage TEST_MESSAGE = PubsubMessage.newBuilder()
      .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8))).build();

  @Test
  public void canReadOneMessage() {
    publish(TEST_MESSAGE);

    AtomicReference<PubsubMessage> received = new AtomicReference<>();
    AtomicReference<Pubsub.Read> input = new AtomicReference<>();

    input.set(new Pubsub.Read(getSubscription(),
        // handler
        message -> CompletableFuture.supplyAsync(() -> message) // create a future with message
            .thenAccept(received::set) // add message to received
            .thenRun(() -> input.get().subscriber.stopAsync()), // stop the subscriber
        // config
        builder -> channelProvider.map(channelProvider -> builder
            .setChannelProvider(channelProvider).setCredentialsProvider(noCredentialsProvider))
            .orElse(builder)));

    input.get().run();

    assertEquals("test", new String(received.get().getData().toByteArray()));
  }

  @Test
  public void canRetryOnException() {
    System.err.println("Causing Exception Warning...");
    String messageId = publish(TEST_MESSAGE);

    List<PubsubMessage> received = new LinkedList<>();
    AtomicReference<Pubsub.Read> input = new AtomicReference<>();

    input.set(new Pubsub.Read(getSubscription(),
        // handler
        message -> CompletableFuture.completedFuture(message) // create a future with message
            .thenAccept(received::add) // add message to received
            .thenRun(() -> {
              // throw an error to nack the message the first time
              if (received.size() == 1) {
                throw new RuntimeException("test");
              }
            }).thenRun(() -> input.get().subscriber.stopAsync()), // stop the subscriber
        // config
        builder -> channelProvider.map(channelProvider -> builder
            .setChannelProvider(channelProvider).setCredentialsProvider(noCredentialsProvider))
            .orElse(builder)));

    input.get().run();

    assertEquals(messageId, received.get(0).getMessageId());
    assertEquals(messageId, received.get(1).getMessageId());
    assertEquals(2, received.size());
  }
}
