package com.mozilla.telemetry.ingestion.sink.util;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;

public abstract class TestWithPubsubResources {

  private String projectId;
  private List<Subscription> subscriptions;
  private TopicAdminClient topicAdminClient;
  private SubscriptionAdminClient subscriptionAdminClient;
  private SubscriberStub subscriber;

  private List<Publisher> publishers;
  protected final Optional<TransportChannelProvider> channelProvider = Optional
      .ofNullable(System.getenv("PUBSUB_EMULATOR_HOST"))
      .map(t -> ManagedChannelBuilder.forTarget(t).usePlaintext().build())
      .map(GrpcTransportChannel::create).map(FixedTransportChannelProvider::create);
  protected final NoCredentialsProvider noCredentialsProvider = NoCredentialsProvider.create();

  protected abstract int numTopics();

  protected String getSubscription(int index) {
    return subscriptions.get(index).getName();
  }

  protected String getTopic(int index) {
    return subscriptions.get(index).getTopic();
  }

  protected String publish(int index, PubsubMessage message) {
    Publisher publisher = publishers.get(index);
    ApiFuture<String> future = publisher.publish(message);
    publisher.publishAllOutstanding();
    try {
      return future.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected List<PubsubMessage> pull(int index, int maxMessages, boolean returnImmediately) {
    List<ReceivedMessage> response = subscriber.pullCallable()
        .call(PullRequest.newBuilder().setMaxMessages(maxMessages)
            .setReturnImmediately(returnImmediately).setSubscription(getSubscription(index))
            .build())
        .getReceivedMessagesList();

    if (response.size() > 0) {
      subscriber.acknowledgeCallable()
          .call(AcknowledgeRequest.newBuilder().setSubscription(getSubscription(index))
              .addAllAckIds(
                  response.stream().map(ReceivedMessage::getAckId).collect(Collectors.toList()))
              .build());
    }

    return response.stream().map(ReceivedMessage::getMessage).collect(Collectors.toList());
  }

  /**
   * Create a Pub/Sub topic and subscription.
   *
   * @throws IOException if Pub/Sub is unavailable
   */
  @Before
  public void initializePubsubResources() throws IOException {
    TopicAdminSettings.Builder topicAdminSettings = TopicAdminSettings.newBuilder();
    SubscriptionAdminSettings.Builder subscriptionAdminSettings = SubscriptionAdminSettings
        .newBuilder();
    SubscriberStubSettings.Builder subscriberStubSettings = SubscriberStubSettings.newBuilder();
    if (channelProvider.isPresent()) {
      topicAdminSettings = topicAdminSettings //
          .setCredentialsProvider(noCredentialsProvider)
          .setTransportChannelProvider(channelProvider.get());
      subscriptionAdminSettings = subscriptionAdminSettings //
          .setCredentialsProvider(noCredentialsProvider)
          .setTransportChannelProvider(channelProvider.get());
      subscriberStubSettings = subscriberStubSettings //
          .setCredentialsProvider(noCredentialsProvider)
          .setTransportChannelProvider(channelProvider.get());
      projectId = "test";
    } else {
      projectId = ServiceOptions.getDefaultProjectId();
    }

    subscriptions = IntStream.range(0, numTopics()).mapToObj(i -> Subscription.newBuilder()
        .setName("projects/" + projectId + "/subscriptions/test-subscription-"
            + UUID.randomUUID().toString())
        .setTopic("projects/" + projectId + "/topics/test-topic-" + UUID.randomUUID().toString())
        .build()).collect(Collectors.toList());

    topicAdminClient = TopicAdminClient.create(topicAdminSettings.build());
    subscriptions.stream().parallel()
        .forEach(subscription -> topicAdminClient.createTopic(subscription.getTopic()));

    subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings.build());
    subscriptions.stream().parallel()
        .forEach(subscription -> subscriptionAdminClient.createSubscription(subscription.getName(),
            subscription.getTopic(), PushConfig.getDefaultInstance(), 0));

    publishers = subscriptions.stream().parallel().map(subscription -> {
      Publisher.Builder publisherBuilder = Publisher.newBuilder(subscription.getTopic());
      if (channelProvider.isPresent()) {
        publisherBuilder = publisherBuilder //
            .setChannelProvider(channelProvider.get())
            .setCredentialsProvider(noCredentialsProvider);
      }
      try {
        return publisherBuilder.build();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).collect(Collectors.toList());

    subscriber = GrpcSubscriberStub.create(subscriberStubSettings.build());
  }

  /** Clean up all the Pub/Sub resources we created. */
  @After
  public void deletePubsubResources() {
    subscriptions.forEach(subscription -> topicAdminClient.deleteTopic(subscription.getTopic()));
    subscriptions.forEach(
        subscription -> subscriptionAdminClient.deleteSubscription(subscription.getName()));
    publishers.forEach(publisher -> {
      try {
        publisher.shutdown();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
