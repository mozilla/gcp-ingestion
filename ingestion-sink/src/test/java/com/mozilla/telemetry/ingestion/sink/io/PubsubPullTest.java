package com.mozilla.telemetry.ingestion.sink.io;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.LeaseManager;
import com.mozilla.telemetry.ingestion.sink.util.MockPubsub;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class PubsubPullTest {

  private static final PubsubMessage TEST_MESSAGE = PubsubMessage.newBuilder()
      .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8))).build();

  @Rule
  public final LoggerContextRule logs = new LoggerContextRule("log4j2-test.yaml");

  private MockPubsub pubsub;

  /** Prepare a mock BQ response. */
  @Before
  public void mockPubsubClient() {
    pubsub = new MockPubsub().withSubscription("subscription", 600);
    pubsub.withAcknowledge().withPullResponse(TEST_MESSAGE, "1");
  }

  private <T> Pubsub.Pull getInput(Function<PubsubMessage, CompletableFuture<T>> output) {
    return new Pubsub.Pull(pubsub.client, "subscription", output, m -> m, ForkJoinPool.commonPool(),
        1, 3, TEST_MESSAGE.getSerializedSize() * 3,
        new LeaseManager(pubsub.client, "subscription", Duration.ofSeconds(600),
            Duration.ofSeconds(1), Duration.ofSeconds(1), ForkJoinPool.commonPool()));
  }

  @Test
  public void canReadOneMessage() {
    AtomicReference<PubsubMessage> received = new AtomicReference<>();
    AtomicReference<Input> input = new AtomicReference<>();

    input.set(getInput(
        // create a future with message
        message -> CompletableFuture.completedFuture(message)
            // add message to received
            .thenAccept(received::set)
            // stop the subscriber
            .thenRun(() -> input.get().stop())));

    input.get().run();
    assertEquals(TEST_MESSAGE, received.get());
  }

  @Test
  public void canHandleException() {
    pubsub.withModifyAckDeadline() //
        .withModifyAckDeadline() //
        .withPullResponse(TEST_MESSAGE, "2") //
        .withPullResponse(TEST_MESSAGE, "3");
    List<PubsubMessage> received = new LinkedList<>();
    AtomicReference<Pubsub.Pull> input = new AtomicReference<>();

    input.set(getInput(message -> {
      received.add(message);
      switch (received.size()) {
        // throw an immediate error to nack the first message
        case 1:
          throw new RuntimeException("first");
          // throw a future error to nack the second message
        case 2:
          return CompletableFuture.completedFuture(null).thenRun(() -> {
            throw new RuntimeException("second");
          });
        // succeed and stop on the third message
        default:
          input.get().stop();
          return CompletableFuture.completedFuture(null);
      }
    }));

    input.get().run();
    assertEquals(Collections.nCopies(3, TEST_MESSAGE), received);
    assertThat(logs.getListAppender("STDOUT").getMessages(),
        contains(containsString("java.lang.RuntimeException: first"),
            containsString("java.lang.RuntimeException: second")));
    assertEquals(ImmutableList.of(0), pubsub.modifyAckDeadlineCallable.requests.stream()
        .map(ModifyAckDeadlineRequest::getAckDeadlineSeconds).collect(Collectors.toList()));
    assertEquals(ImmutableList.of(ImmutableList.of("1", "2")),
        pubsub.modifyAckDeadlineCallable.requests.stream()
            .map(ModifyAckDeadlineRequest::getAckIdsList).collect(Collectors.toList()));
    assertEquals(ImmutableList.of(ImmutableList.of("3")), pubsub.acknowledgeCallable.requests
        .stream().map(AcknowledgeRequest::getAckIdsList).collect(Collectors.toList()));
  }
}
