package com.mozilla.telemetry.integration;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.Topic;
import com.google.cloud.pubsublite.proto.Topic.PartitionConfig.Capacity;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.InputType;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.options.OutputType;
import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test suite that accesses Pub/Sub Lite in GCP.
 *
 * <p>Because this requires credentials, this suite is excluded by default in the surefire
 * configuration, but can be enabled by passing command-line option
 * {@code -Dtest=PubsubLiteIntegrationTest}. Credentials can be provided by initializing a
 * configuration in the gcloud command-line tool or by providing a path to service account
 * credentials in environment variable {@code GOOGLE_APPLICATION_CREDENTIALS}.
 *
 * <p>The provided credentials are assumed to have "Pub/Sub Admin" privileges.
 */
public class PubsubLiteIntegrationTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private final CloudRegion region = CloudRegion.of("us-central1");
  private final CloudZone zone = CloudZone.of(region, 'a');
  private final AdminClient adminClient;

  public PubsubLiteIntegrationTest() throws Exception {
    adminClient = AdminClient.create(AdminClientSettings.newBuilder().setRegion(region).build());
  }

  private TopicPath topicPath = null;
  private SubscriptionPath subscriptionPath = null;

  /** Create a Pub/Sub Lite topic and subscription. */
  @Before
  public void initializePubsubResources() throws Exception {
    final Topic topic = adminClient.createTopic(Topic.newBuilder()
        .setPartitionConfig(Topic.PartitionConfig.newBuilder()
            .setCapacity(
                Capacity.newBuilder().setPublishMibPerSec(4).setSubscribeMibPerSec(4).build())
            .setCount(1).build())
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
  }

  /**
   * Clean up all the Pub/Sub Lite resources we created.
   */
  @After
  public void deletePubsubResources() throws Exception {
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

  @Test
  public void readPubsubInput() throws Exception {
    List<String> inputLines = Lines.resources("testdata/basic-messages-nonempty.ndjson");
    publishLines(inputLines);

    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    SinkOptions.Parsed sinkOptions = pipeline.getOptions().as(SinkOptions.Parsed.class);
    sinkOptions.setInput(StaticValueProvider.of(subscriptionPath.toString()));

    PCollection<String> output = pipeline.apply(InputType.pubsub_lite.read(sinkOptions))
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(inputLines);

    // This runs in the background and returns immediately due to setBlockOnRun above.
    PipelineResult result = pipeline.run();

    // The wait here is determined empirically; it's not entirely clear why it takes this long.
    System.err.println("Waiting 15 seconds to make sure we've processed all messages...");
    result.waitUntilFinish(org.joda.time.Duration.millis(15000));
    System.err.println("Done waiting; now cancelling the pipeline so the test can finish.");
    result.cancel();
  }

  @Test(timeout = 90_000L)
  public void canOutput() throws Exception {
    final SinkOptions.Parsed sinkOptions = pipeline.getOptions().as(SinkOptions.Parsed.class);
    // We would normally use pipeline.newProvider instead of StaticValueProvider in tests,
    // but output has to be available during initialization for Pub/Sub Lite, and the value of
    // pipeline.newProvider isn't available until after the pipeline starts.
    sinkOptions.setOutput(StaticValueProvider.of(topicPath.toString()));
    // We would normally use pipeline.newProvider instead of StaticValueProvider in tests,
    // but something about this configuration causes the pipeline to stall when CompressPayload
    // accesses a method on the underlying enum value when defined via pipeline.newProvider.
    sinkOptions.setOutputPubsubCompression(StaticValueProvider.of(Compression.UNCOMPRESSED));

    final List<String> payloads = ImmutableList.of("passing", StringUtils.repeat("f", 1_000_000));

    pipeline //
        .apply(Create.of(payloads)) //
        .apply(InputFileFormat.text.decode()) //
        .apply(OutputType.pubsub_lite.write(sinkOptions));
    pipeline.run();

    System.err.println("Waiting for subscriber to receive messages published in the pipeline...");
    List<String> received = receivePayloads(payloads.size());
    assertThat(received, matchesInAnyOrder(payloads));
  }

  /*
   * Helper methods
   */

  private void publishLines(List<String> lines) throws Exception {
    Publisher publisher = Publisher
        .create(PublisherSettings.newBuilder().setTopicPath(topicPath).build());
    try {
      publisher.startAsync().awaitRunning();
      for (String line : lines) {
        PubsubMessage message = PubsubConstraints.ensureNonNull(Json.readPubsubMessage(line));
        com.google.pubsub.v1.PubsubMessage outgoing = com.google.pubsub.v1.PubsubMessage
            .newBuilder().setData(ByteString.copyFrom(message.getPayload()))
            .putAllAttributes(message.getAttributeMap()).build();
        publisher.publish(outgoing).get();
      }
    } finally {
      publisher.stopAsync().awaitTerminated();
    }
  }

  private List<String> receivePayloads(int expectedMessageCount) throws Exception {
    final List<String> received = new CopyOnWriteArrayList<>();
    final AtomicReference<Subscriber> subscriber = new AtomicReference<>();
    subscriber
        .set(Subscriber.create(SubscriberSettings.newBuilder().setSubscriptionPath(subscriptionPath)
            .setPerPartitionFlowControlSettings(FlowControlSettings.builder()
                .setMessagesOutstanding(1_000L).setBytesOutstanding(3_500_000L).build())
            .setReceiver((message, consumer) -> {
              received.add(message.getData().toStringUtf8());
              consumer.ack();
              if (received.size() >= expectedMessageCount) {
                subscriber.get().stopAsync();
              }
            }).build()));
    subscriber.get().startAsync();
    subscriber.get().awaitTerminated();
    return received;
  }
}
