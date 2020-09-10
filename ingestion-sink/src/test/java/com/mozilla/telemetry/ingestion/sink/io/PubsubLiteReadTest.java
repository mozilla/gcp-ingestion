package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.StatusException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class PubsubLiteReadTest {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private Subscriber subscriber;

  @Mock
  private SubscriberSettings subscriberSettings;

  @Mock
  private AckReplyConsumer ackReplyConsumer;

  @Mock
  private SubscriberSettings.Builder subscriberSettingsBuilder;

  private static final PubsubMessage TEST_MESSAGE = PubsubMessage.newBuilder()
      .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8))).build();

  @Test
  public void canReadOneMessage() throws Exception {
    when(subscriberSettingsBuilder.build()).thenReturn(subscriberSettings);
    // package-private method
    final Method instantiate = SubscriberSettings.class.getDeclaredMethod("instantiate");
    instantiate.setAccessible(true);
    when(instantiate.invoke(subscriberSettings)).thenReturn(subscriber);
    final AtomicReference<PubsubMessage> received = new AtomicReference<>();
    final AtomicReference<Input> input = new AtomicReference<>();
    final AtomicReference<SubscriberSettings.Builder> settingsBuilder = new AtomicReference<>();

    input.set(new PubsubLite.Read("projects/x/locations/x-x-x/subscriptions/test", 1L, 1L,
        // handler
        message -> CompletableFuture.completedFuture(message) // create a future with message
            .thenAccept(received::set) // add message to received
            .thenRun(() -> input.get().stopAsync()), // stop the subscriber
        // config
        builder -> {
          settingsBuilder.set(builder);
          return subscriberSettingsBuilder;
        }, m -> m));

    input.get().run();

    verify(subscriber, times(1)).startAsync();
    verify(subscriber, times(1)).awaitTerminated();
    verify(subscriber, times(1)).stopAsync();

    final SubscriberSettings settings = settingsBuilder.get().build();
    // package-private method
    final Method receiver = SubscriberSettings.class.getDeclaredMethod("receiver");
    receiver.setAccessible(true);
    ((MessageReceiver) receiver.invoke(settings)).receiveMessage(TEST_MESSAGE, ackReplyConsumer);

    assertEquals(TEST_MESSAGE, received.get());
    verify(subscriber, times(2)).stopAsync();
  }

  @Test
  public void throwsInvalidSubscription() {
    RuntimeException thrown = assertThrows(RuntimeException.class,
        () -> new PubsubLite.Read("projects/123/subscriptions/test-123", 1, 1,
            m -> CompletableFuture.completedFuture(null), b -> b, m -> m));
    assertEquals(StatusException.class, thrown.getCause().getClass());
    assertEquals("INVALID_ARGUMENT", thrown.getCause().getMessage());
  }
}
