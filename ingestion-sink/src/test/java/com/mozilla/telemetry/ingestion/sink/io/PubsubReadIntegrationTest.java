package com.mozilla.telemetry.ingestion.sink.io;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BatchException;
import com.mozilla.telemetry.ingestion.sink.util.SinglePubsubTopic;
import com.mozilla.telemetry.ingestion.sink.util.TimedFuture;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;

public class PubsubReadIntegrationTest {

  private static final PubsubMessage TEST_MESSAGE = PubsubMessage.newBuilder()
      .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8))).build();

  @Rule
  public final SinglePubsubTopic pubsub = new SinglePubsubTopic();

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
    // Set up a ListAppender on the actual LoggerContext (not an isolated test context)
    // This ensures we capture logs from Pubsub.Read's static logger
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    ListAppender listAppender = new ListAppender("TestListAppender", null, JsonLayout.newBuilder()
        .setCompact(true).setEventEol(true).setStacktraceAsString(true).build(), false, false);
    listAppender.start();
    LoggerConfig rootLogger = config.getRootLogger();
    rootLogger.addAppender(listAppender, Level.WARN, null);
    ctx.updateLoggers();

    try {
      final String messageId = pubsub.publish(TEST_MESSAGE).get(0);

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

      // Assert that batch exception logged once, and other exception logged every time
      assertThat(listAppender.getMessages(),
          contains(containsString("java.lang.RuntimeException: batch"),
              containsString("java.lang.RuntimeException: single"),
              containsString("failed message type: ?/?"),
              containsString("java.lang.RuntimeException: single"),
              containsString("failed message type: ?/?")));
    } finally {
      // Clean up: remove the appender
      rootLogger.removeAppender("TestListAppender");
      listAppender.stop();
      ctx.updateLoggers();
    }
  }

  @Rule
  public final SystemErrRule systemErr = new SystemErrRule();

  @Test
  public void canHoldLeases() {
    // publish enough messages to exceed a single ModifyAckDeadline request
    int sentCount = 2775;
    PubsubMessage[] messages = new PubsubMessage[sentCount];
    Arrays.fill(messages, TEST_MESSAGE);
    pubsub.publish(messages);

    // pull all messages and hold them for 30 seconds
    final CompletableFuture<Void> done = new TimedFuture(Duration.ofSeconds(30));
    final MessageReceiver receiver = (message, consumer) -> {
      done.whenComplete((result, exception) -> {
        if (exception == null) {
          consumer.ack();
        } else {
          consumer.nack();
        }
      });
    };

    final Subscriber subscriber = Subscriber
        .newBuilder(ProjectSubscriptionName.parse(pubsub.getSubscription()), receiver)
        .setFlowControlSettings(
            FlowControlSettings.newBuilder().setMaxOutstandingElementCount((long) sentCount)
                .setMaxOutstandingRequestBytes(30_000_000L).build())
        .build();
    done.whenComplete((v, e) -> subscriber.stopAsync());

    systemErr.enableLog();
    try {
      subscriber.startAsync();
      subscriber.awaitTerminated();
    } finally {
      subscriber.stopAsync();
    }
    assertEquals(systemErr.getLog(), "");
  }
}
