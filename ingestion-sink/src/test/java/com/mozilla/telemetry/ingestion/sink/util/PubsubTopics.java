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
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * JUnit Rule for reusing code to interact with Pub/Sub.
 */
public class PubsubTopics extends TestWatcher {

  public int numTopics;

  public PubsubTopics(int numTopics) {
    this.numTopics = numTopics;
  }

  private String projectId;
  private TopicAdminClient topicAdminClient;
  private SubscriptionAdminClient subscriptionAdminClient;
  private SubscriberStub subscriber;
  private List<Subscription> subscriptions;
  private List<Publisher> publishers;

  public Optional<TransportChannelProvider> channelProvider = Optional
      .ofNullable(System.getenv("PUBSUB_EMULATOR_HOST"))
      .map(t -> ManagedChannelBuilder.forTarget(t).usePlaintext().build())
      .map(GrpcTransportChannel::create).map(FixedTransportChannelProvider::create);

  public NoCredentialsProvider noCredentialsProvider = NoCredentialsProvider.create();

  public String getSubscription(int index) {
    return subscriptions.get(index).getName();
  }

  public String getTopic(int index) {
    return subscriptions.get(index).getTopic();
  }

  /**
   * Publish a {@link PubsubMessage} to the topic indicated by {@code index}.
   */
  public String publish(int index, PubsubMessage message) {
    Publisher publisher = publishers.get(index);
    ApiFuture<String> future = publisher.publish(message);
    publisher.publishAllOutstanding();
    try {
      return future.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Pull and ack {@link PubsubMessage}s from the subscription indicated by {@code index}.
   */
  public List<PubsubMessage> pull(int index, int maxMessages, boolean returnImmediately) {
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

  /** Create a Pub/Sub topics and subscriptions. */
  @Override
  protected void starting(Description description) {

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

    // generate subscription configurations
    subscriptions = IntStream.range(0, numTopics).mapToObj(i -> Subscription.newBuilder()
        .setName("projects/" + projectId + "/subscriptions/test-subscription-"
            + UUID.randomUUID().toString())
        .setTopic("projects/" + projectId + "/topics/test-topic-" + UUID.randomUUID().toString())
        .build()).collect(Collectors.toList());

    // create clients
    try {
      topicAdminClient = TopicAdminClient.create(topicAdminSettings.build());
      subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings.build());
      subscriber = GrpcSubscriberStub.create(subscriberStubSettings.build());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // create topics
    subscriptions.stream().parallel()
        .forEach(subscription -> topicAdminClient.createTopic(subscription.getTopic()));

    // create subscriptions
    subscriptions.stream().parallel()
        .forEach(subscription -> subscriptionAdminClient.createSubscription(subscription.getName(),
            subscription.getTopic(), PushConfig.getDefaultInstance(), 0));

    // create publishers
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
  }

  /** Clean up all the Pub/Sub resources we created. */
  @Override
  protected void finished(Description description) {
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
