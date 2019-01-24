/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.integration;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.InputType;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.options.OutputType;
import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test suite that accesses Pub/Sub in GCP.
 *
 * <p>Because this requires credentials, this suite is excluded by default in the surefire
 * configuration, but can be enabled by passing command-line option
 * {@code -Dtest=PubsubIntegrationTest}. Credentials can be provided by initializing a
 * configuration in the gcloud command-line tool or by providing a path to service account
 * credentials in environment variable {@code GOOGLE_APPLICATION_CREDENTIALS}.
 *
 * <p>The provided credentials are assumed to have "Pub/Sub Admin" privileges.
 *
 * <p>We contact real Pub/Sub here rather than using the emulator because Google's Java SDK
 * lacks built-in support for the emulator. Using the emulator is possible, but requires
 * significant configuration code, as seen in
 * <a href="https://github.com/googleapis/google-cloud-java/blob/d971c35c944a5a9e200ac4c94ca926024c71677c/TESTING.md#testing-code-that-uses-pubsub">TESTING.md</a>.
 */
public class PubsubIntegrationTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private String projectId;
  private String topicId;
  private String subscriptionId;
  private ProjectTopicName topicName;
  private ProjectSubscriptionName subscriptionName;

  /**
   * Create a Pub/Sub topic and subscription.
   */
  @Before
  public void initializePubsubResources() throws Exception {
    projectId = ServiceOptions.getDefaultProjectId();
    topicId = "test-topic-" + UUID.randomUUID().toString();
    subscriptionId = "test-subscription-" + UUID.randomUUID().toString();
    topicName = ProjectTopicName.of(projectId, topicId);
    subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

    TopicAdminClient.create().createTopic(topicName);
    SubscriptionAdminClient.create().createSubscription(subscriptionName, topicName,
        PushConfig.getDefaultInstance(), 0);
  }

  /** Clean up all the Pub/Sub resources we created. */
  @After
  public void deletePubsubResources() throws Exception {
    SubscriptionAdminClient.create().deleteSubscription(subscriptionName);
    TopicAdminClient.create().deleteTopic(topicName);
  }

  @Test(timeout = 30000)
  public void canReadPubsubInput() throws Exception {
    List<String> inputLines = Lines.resources("testdata/basic-messages-nonempty.ndjson");
    publishLines(inputLines);

    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    SinkOptions.Parsed sinkOptions = pipeline.getOptions().as(SinkOptions.Parsed.class);
    sinkOptions.setInput(pipeline.newProvider(subscriptionName.toString()));

    PCollection<String> output = pipeline.apply(InputType.pubsub.read(sinkOptions)).output()
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(inputLines);

    // This runs in the background and returns immediately due to setBlockOnRun above.
    PipelineResult result = pipeline.run();

    // The wait here is determined empirically; it's not entirely clear why it takes this long.
    System.err.println("Waiting 15 seconds to make sure we've processed all messages...");
    result.waitUntilFinish(Duration.millis(15000));
    System.err.println("Done waiting; now cancelling the pipeline so the test can finish.");
    result.cancel();
  }

  @Test(timeout = 30000)
  public void canSendPubsubOutput() throws Exception {
    final List<String> inputLines = Lines.resources("testdata/pubsub-integration/input.ndjson");

    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    SinkOptions.Parsed sinkOptions = pipeline.getOptions().as(SinkOptions.Parsed.class);
    sinkOptions.setOutput(pipeline.newProvider(topicName.toString()));
    sinkOptions.setOutputPubsubCompression(pipeline.newProvider(Compression.UNCOMPRESSED));

    pipeline.apply(Create.of(inputLines)).apply(InputFileFormat.json.decode()).output()
        .apply(OutputType.pubsub.write(sinkOptions));

    final PipelineResult result = pipeline.run();

    System.err.println("Waiting for subscriber to receive messages published in the pipeline...");
    List<String> expectedLines = Lines.resources("testdata/pubsub-integration/truncated.ndjson");
    List<String> received = receiveLines(expectedLines.size());
    assertThat(received, matchesInAnyOrder(expectedLines));
    result.cancel();
  }

  @Test(timeout = 30000)
  public void canSendGzippedPayloads() throws Exception {
    final List<String> inputLines = Lines.resources("testdata/pubsub-integration/input.ndjson");

    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    SinkOptions sinkOptions = pipeline.getOptions().as(SinkOptions.class);
    sinkOptions.setOutputType(OutputType.pubsub);
    sinkOptions.setOutput(pipeline.newProvider(topicName.toString()));
    SinkOptions.Parsed options = SinkOptions.parseSinkOptions(sinkOptions);

    pipeline.apply(Create.of(inputLines)).apply(InputFileFormat.json.decode()).output()
        .apply(options.getOutputType().write(options));

    final PipelineResult result = pipeline.run();

    System.err.println("Waiting for subscriber to receive messages published in the pipeline...");
    List<String> expectedLines = Lines.resources("testdata/pubsub-integration/gzipped.ndjson");
    List<String> received = receiveLines(expectedLines.size());
    assertThat(received, matchesInAnyOrder(expectedLines));
    result.cancel();
  }

  /*
   * Helper methods
   */

  private void publishLines(List<String> lines) throws Exception {
    Publisher publisher = Publisher.newBuilder(topicName).build();
    lines.forEach(line -> {
      try {
        org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage msg = Json.readPubsubMessage(line);
        Map<String, String> attributes = Optional.ofNullable(msg.getAttributeMap())
            .orElse(ImmutableMap.of());
        com.google.pubsub.v1.PubsubMessage outgoing = com.google.pubsub.v1.PubsubMessage
            .newBuilder().putAllAttributes(attributes)
            .setData(ByteString.copyFrom(msg.getPayload())).build();
        publisher.publish(outgoing);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    publisher.shutdown();
  }

  private List<String> receiveLines(int expectedMessageCount) throws Exception {
    List<String> received = new CopyOnWriteArrayList<>();
    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId,
        subscriptionId);

    MessageReceiver receiver = new MessageReceiver() {

      @Override
      public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        try {
          String encoded = Json.asString(new org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage(
              message.getData().toByteArray(), message.getAttributesMap()));
          received.add(encoded);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        consumer.ack();
      }
    };
    Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
    subscriber.startAsync();
    while (received.size() < expectedMessageCount) {
      Thread.sleep(100);
    }
    subscriber.stopAsync();

    return received;
  }

}
