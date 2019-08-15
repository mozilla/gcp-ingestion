/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.util;

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
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;

public abstract class TestWithPubsubResources {

  public String projectId;
  public String topicId;
  public String subscriptionId;
  public ProjectTopicName topicName;
  public ProjectSubscriptionName subscriptionName;
  public TopicAdminClient topicAdminClient;
  public SubscriptionAdminClient subscriptionAdminClient;

  public Publisher publisher;
  public final Optional<TransportChannelProvider> channelProvider = Optional
      .ofNullable(System.getenv("PUBSUB_EMULATOR_HOST"))
      .map(t -> ManagedChannelBuilder.forTarget(t).usePlaintext().build())
      .map(GrpcTransportChannel::create).map(FixedTransportChannelProvider::create);
  public final NoCredentialsProvider noCredentialsProvider = NoCredentialsProvider.create();

  /** Create a Pub/Sub topic and subscription. */
  @Before
  public void initializePubsubResources() throws IOException {
    TopicAdminSettings.Builder topicAdminSettings = TopicAdminSettings.newBuilder();
    SubscriptionAdminSettings.Builder subscriptionAdminSettings = SubscriptionAdminSettings
        .newBuilder();
    if (channelProvider.isPresent()) {
      topicAdminSettings = topicAdminSettings //
          .setCredentialsProvider(noCredentialsProvider)
          .setTransportChannelProvider(channelProvider.get());
      subscriptionAdminSettings = subscriptionAdminSettings //
          .setCredentialsProvider(noCredentialsProvider)
          .setTransportChannelProvider(channelProvider.get());
      projectId = "test";
    } else {
      projectId = ServiceOptions.getDefaultProjectId();
    }
    topicAdminClient = TopicAdminClient.create(topicAdminSettings.build());
    subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings.build());

    topicId = "test-topic-" + UUID.randomUUID().toString();
    subscriptionId = "test-subscription-" + UUID.randomUUID().toString();
    topicName = ProjectTopicName.of(projectId, topicId);
    subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

    topicAdminClient.createTopic(topicName);
    subscriptionAdminClient.createSubscription(subscriptionName, topicName,
        PushConfig.getDefaultInstance(), 0);

    Publisher.Builder publisherBuilder = Publisher.newBuilder(topicName);
    if (channelProvider.isPresent()) {
      publisherBuilder = publisherBuilder //
          .setChannelProvider(channelProvider.get()).setCredentialsProvider(noCredentialsProvider);
    }
    publisher = publisherBuilder.build();
  }

  /** Clean up all the Pub/Sub resources we created. */
  @After
  public void deletePubsubResources() {
    topicAdminClient.deleteTopic(topicName);
    subscriptionAdminClient.deleteSubscription(subscriptionName);
  }
}
