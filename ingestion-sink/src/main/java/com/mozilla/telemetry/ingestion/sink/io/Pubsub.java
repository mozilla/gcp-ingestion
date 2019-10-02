package com.mozilla.telemetry.ingestion.sink.io;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pubsub {

  private Pubsub() {
  }

  public static class Read {

    private static final Logger LOG = LoggerFactory.getLogger(Read.class);

    @VisibleForTesting
    public Subscriber subscriber;

    public <T> Read(String subscriptionName, Function<PubsubMessage, CompletableFuture<T>> output,
        Function<Subscriber.Builder, Subscriber.Builder> config) {
      ProjectSubscriptionName subscription = ProjectSubscriptionName.parse(subscriptionName);
      subscriber = config.apply(Subscriber.newBuilder(subscription,
          (message, consumer) -> CompletableFuture.supplyAsync(() -> message)
              .thenComposeAsync(output).whenCompleteAsync((result, exception) -> {
                if (exception == null) {
                  consumer.ack();
                } else {
                  LOG.warn("Exception while attempting to deliver message:", exception);
                  consumer.nack();
                }
              })))
          .build();
    }

    public void run() {
      try {
        subscriber.startAsync();
        subscriber.awaitTerminated();
      } finally {
        subscriber.stopAsync();
      }
    }
  }

  public static class Write implements Function<PubsubMessage, CompletableFuture<String>> {

    private final Executor executor;
    private final Function<Publisher.Builder, Publisher.Builder> config;
    private final PubsubMessageToTemplatedString topicTemplate;
    private final ConcurrentMap<String, Publisher> publishers = new ConcurrentHashMap<>();

    public Write(String topicTemplate, int numThreads,
        Function<Publisher.Builder, Publisher.Builder> config) {
      executor = Executors.newFixedThreadPool(numThreads);
      this.topicTemplate = new PubsubMessageToTemplatedString(topicTemplate);
      this.config = config;
    }

    private Publisher getPublisher(PubsubMessage message) {
      return publishers.compute(topicTemplate.apply(message), (topic, publisher) -> {
        if (publisher == null) {
          try {
            return config.apply(Publisher.newBuilder(ProjectTopicName.parse(topic))).build();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
        return publisher;
      });
    }

    @Override
    public CompletableFuture<String> apply(PubsubMessage message) {
      final ApiFuture<String> future = getPublisher(message).publish(message);
      final CompletableFuture<String> result = new CompletableFuture<>();
      ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

        @Override
        public void onFailure(Throwable throwable) {
          result.completeExceptionally(throwable);
        }

        @Override
        public void onSuccess(String messageId) {
          result.complete(messageId);
        }
      }, executor);
      return result;
    }

    public CompletableFuture<Void> withoutResult(PubsubMessage message) {
      return apply(message).thenAccept(result -> {
      });
    }
  }
}
