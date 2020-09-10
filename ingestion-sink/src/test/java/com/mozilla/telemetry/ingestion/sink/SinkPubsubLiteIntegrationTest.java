package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.Topic;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BoundedSink;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.contrib.java.lang.system.SystemOutRule;

public class SinkPubsubLiteIntegrationTest {

  private final CloudRegion region = CloudRegion.of("us-central1");
  private final CloudZone zone = CloudZone.of(region, 'a');
  private final AdminClient adminClient;

  public SinkPubsubLiteIntegrationTest() throws Exception {
    adminClient = AdminClient.create(AdminClientSettings.newBuilder().setRegion(region).build());
  }

  private TopicPath topicPath = null;
  private SubscriptionPath subscriptionPath = null;
  private Publisher publisher;

  /** Create Pub/Sub Lite subscription. */
  @Before
  public void createSubscription() throws Exception {
    final Topic topic = adminClient.createTopic(Topic.newBuilder()
        .setPartitionConfig(Topic.PartitionConfig.newBuilder().setScale(1).setCount(1).build())
        .setRetentionConfig(Topic.RetentionConfig.newBuilder()
            // 1 hour
            .setPeriod(com.google.protobuf.Duration.newBuilder()
                .setSeconds(Duration.ofHours(1).getSeconds()).build())
            // minimum value 30 GiB
            .setPerPartitionBytes(30L * 1024L * 1024L * 1024L).build())
        .setName(TopicPath.newBuilder().setLocation(zone)
            .setName(TopicName.of("test-topic-" + UUID.randomUUID().toString()))
            .setProject(ProjectId.of(ServiceOptions.getDefaultProjectId())).build().toString())
        .build()).get();
    // get resolved topic from result of createTopic, so that project id has been converted to
    // project number, because project id is not allowed in Subscription.Builder.setTopic
    topicPath = TopicPath.parse(topic.getName());
    final Subscription subscription = adminClient.createSubscription(Subscription.newBuilder()
        .setDeliveryConfig(Subscription.DeliveryConfig.newBuilder()
            .setDeliveryRequirement(
                Subscription.DeliveryConfig.DeliveryRequirement.DELIVER_IMMEDIATELY)
            .build())
        .setName(topic.getName().replace("topic", "subscription")).setTopic(topic.getName())
        .build()).get();
    subscriptionPath = SubscriptionPath.parse(subscription.getName());
    publisher = Publisher.create(PublisherSettings.newBuilder().setTopicPath(topicPath).build());
  }

  /** Delete Pub/Sub Lite subscription. */
  @After
  public void deleteSubscription() throws Exception {
    try {
      if (topicPath != null) {
        adminClient.deleteTopic(topicPath).get();
      }
      if (subscriptionPath != null) {
        adminClient.deleteSubscription(subscriptionPath).get();
      }
    } finally {
      topicPath = null;
      subscriptionPath = null;
    }
  }

  @Rule
  public final SystemOutRule systemOut = new SystemOutRule();

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  private static final PubsubMessage TEST_MESSAGE = PubsubMessage.newBuilder()
      .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8))).build();

  @Test
  public void canReadOneMessage() throws Exception {
    try {
      publisher.startAsync().awaitRunning();
      publisher.publish(TEST_MESSAGE).get();
    } finally {
      publisher.stopAsync().awaitTerminated();
    }

    environmentVariables.set("INPUT_SUBSCRIPTION", subscriptionPath.toString());
    environmentVariables.set("OUTPUT_PIPE", "-");
    systemOut.enableLog();
    systemOut.mute();
    BoundedSink.run(1, 90);

    assertEquals("{\"payload\":\"dGVzdA==\"}\n", systemOut.getLog());
  }
}
