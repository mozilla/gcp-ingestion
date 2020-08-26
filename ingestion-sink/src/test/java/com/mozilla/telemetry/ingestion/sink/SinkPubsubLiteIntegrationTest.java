package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.TopicPaths;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.Topic;
import com.google.cloud.resourcemanager.ResourceManagerOptions;
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
  private ProjectNumber projectNumber;
  private TopicPath topicPath;
  private SubscriptionPath subscriptionPath;
  private AdminClient adminClient;
  private Publisher publisher;

  /** Create pubsub lite subscription. */
  @Before
  public void createSubscription() throws Exception {
    projectNumber = ProjectNumber.of(ResourceManagerOptions.getDefaultInstance().getService()
        .get(ServiceOptions.getDefaultProjectId()).getProjectNumber());
    topicPath = TopicPaths.newBuilder().setZone(zone)
        .setTopicName(TopicName.of("test-topic-" + UUID.randomUUID().toString()))
        .setProjectNumber(projectNumber).build();
    final Topic topic = Topic.newBuilder()
        .setPartitionConfig(Topic.PartitionConfig.newBuilder().setScale(1).setCount(1).build())
        .setRetentionConfig(Topic.RetentionConfig.newBuilder()
            // 1 hour
            .setPeriod(com.google.protobuf.Duration.newBuilder()
                .setSeconds(Duration.ofHours(1).getSeconds()).build())
            // minimum value 30 GiB
            .setPerPartitionBytes(30L * 1024L * 1024L * 1024L).build())
        .setName(topicPath.value()).build();
    subscriptionPath = SubscriptionPath.of(topicPath.value().replace("topic", "subscription"));
    final Subscription subscription = Subscription.newBuilder()
        .setDeliveryConfig(Subscription.DeliveryConfig.newBuilder()
            .setDeliveryRequirement(
                Subscription.DeliveryConfig.DeliveryRequirement.DELIVER_IMMEDIATELY)
            .build())
        .setName(subscriptionPath.value()).setTopic(topic.getName()).build();
    adminClient = AdminClient.create(AdminClientSettings.newBuilder().setRegion(region).build());
    adminClient.createTopic(topic).get();
    try {
      adminClient.createSubscription(subscription).get();
    } catch (Exception e) {
      adminClient.deleteTopic(TopicPath.of(topic.getName()));
      throw e;
    }
    publisher = Publisher.create(PublisherSettings.newBuilder().setTopicPath(topicPath).build());
  }

  @After
  public void deleteSubscription() throws Exception {
    adminClient.deleteTopic(topicPath).get();
    adminClient.deleteSubscription(subscriptionPath).get();
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

    environmentVariables.set("INPUT_SUBSCRIPTION", subscriptionPath.value());
    environmentVariables.set("OUTPUT_PIPE", "-");
    systemOut.enableLog();
    systemOut.mute();
    BoundedSink.run(1, 90);

    assertEquals("{\"payload\":\"dGVzdA==\"}\n", systemOut.getLog());
  }
}
