package com.mozilla.telemetry.ingestion.sink.io;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BatchException;
import com.mozilla.telemetry.ingestion.sink.util.SinglePubsubTopic;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.Rule;
import org.junit.Test;

public class PubsubReadIntegrationTest {

  private static final PubsubMessage TEST_MESSAGE = PubsubMessage.newBuilder()
      .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8))).build();

  @Rule
  public final SinglePubsubTopic pubsub = new SinglePubsubTopic();

  @Rule
  public final LoggerContextRule logs = new LoggerContextRule("log4j2-list.yaml");

  @Test
  public void canReadOneMessage() {
    pubsub.publish(TEST_MESSAGE);

    final AtomicReference<PubsubMessage> received = new AtomicReference<>();
    final AtomicReference<Pubsub.Read> input = new AtomicReference<>();

    input.set(new Pubsub.Read(pubsub.getSubscription(),
        // handler
        message -> CompletableFuture.supplyAsync(() -> message) // create a future with message
            .thenAccept(received::set) // add message to received
            .thenRun(() -> input.get().stopAsync()), // stop the subscriber
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

    final String messageId = pubsub.publish(TEST_MESSAGE);

    final List<PubsubMessage> received = new LinkedList<>();
    final AtomicReference<Pubsub.Read> input = new AtomicReference<>();

    final BatchException batchException = (BatchException) BatchException
        .of(new RuntimeException("batch"), 2);
    final RuntimeException runtimeException = new RuntimeException("single");

    input.set(new Pubsub.Read(pubsub.getSubscription(),
        // handler
        message -> CompletableFuture.completedFuture(message) // create a future with message
            .thenAccept(received::add) // add message to received
            .thenRun(() -> {
              // throw the same batch exception to nack the message the first and second time
              if (received.size() == 1 || received.size() == 2) {
                throw batchException;
              }
              // throw a runtime exception to nack the message the third time
              if (received.size() == 3 || received.size() == 4) {
                throw runtimeException;
              }
            }).thenRun(() -> input.get().subscriber.stopAsync()), // stop the subscriber
        // config
        builder -> pubsub.channelProvider
            .map(channelProvider -> builder.setChannelProvider(channelProvider)
                .setCredentialsProvider(pubsub.noCredentialsProvider))
            .orElse(builder),
        m -> m));

    input.get().run();

    assertEquals(Collections.nCopies(5, messageId),
        received.stream().map(PubsubMessage::getMessageId).collect(Collectors.toList()));
    // assert that batch exception logged once, and other exception logged every time
    assertThat(logs.getListAppender("STDOUT").getMessages(),
        contains(containsString("java.lang.RuntimeException: batch"),
            containsString("java.lang.RuntimeException: single"),
            containsString("java.lang.RuntimeException: single")));
  }
}
