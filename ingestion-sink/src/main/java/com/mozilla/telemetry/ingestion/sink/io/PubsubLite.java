package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.StatusException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubsubLite {

  private PubsubLite() {
  }

  public static class Read implements Input {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubLite.Read.class);

    @VisibleForTesting
    public final Subscriber subscriber;

    /** Constructor. */
    public <T> Read(String subscription, long messagesOutstanding, long bytesOutstanding,
        Function<PubsubMessage, CompletableFuture<T>> output,
        Function<SubscriberSettings.Builder, SubscriberSettings.Builder> config,
        Function<PubsubMessage, PubsubMessage> decompress) {
      try {
        subscriber = Subscriber
            .create(
                config
                    .apply(SubscriberSettings.newBuilder()
                        .setSubscriptionPath(SubscriptionPath.parse(subscription))
                        .setReceiver(Pubsub.getConsumer(LOG, decompress, output))
                        .setPerPartitionFlowControlSettings(FlowControlSettings.builder()
                            .setMessagesOutstanding(messagesOutstanding)
                            .setBytesOutstanding(bytesOutstanding).build()))
                    .build());
      } catch (StatusException e) {
        throw new RuntimeException(e);
      }
    }

    /** Run the subscriber until terminated. */
    public void run() {
      try {
        subscriber.startAsync();
        subscriber.awaitTerminated();
      } finally {
        subscriber.stopAsync();
      }
    }

    @Override
    public void stopAsync() {
      subscriber.stopAsync();
    }
  }
}
