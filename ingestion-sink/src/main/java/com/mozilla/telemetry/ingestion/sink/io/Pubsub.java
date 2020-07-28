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
import com.mozilla.telemetry.ingestion.sink.util.BatchException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pubsub {

  private Pubsub() {
  }

  public static class Read implements Input {

    private static final Logger LOG = LoggerFactory.getLogger(Read.class);

    @VisibleForTesting
    public Subscriber subscriber;

    /** Constructor. */
    public <T> Read(String subscriptionName, Function<PubsubMessage, CompletableFuture<T>> output,
        Function<Subscriber.Builder, Subscriber.Builder> config,
        Function<PubsubMessage, PubsubMessage> decompress) {
      ProjectSubscriptionName subscription = ProjectSubscriptionName.parse(subscriptionName);
      subscriber = config.apply(Subscriber.newBuilder(subscription,
          // Synchronous CompletableFuture methods are executed by the thread that completes the
          // future, or the current thread if the future is already complete. Use that here to
          // minimize memory usage by doing as much work as immediately possible.
          (message, consumer) -> CompletableFuture.completedFuture(message).thenApply(decompress)
              .thenCompose(output).whenComplete((result, exception) -> {
                if (exception == null) {
                  consumer.ack();
                } else {
                  // exception is always a CompletionException caused by another exception
                  if (exception.getCause() instanceof BatchException) {
                    // only log batch exception once
                    ((BatchException) exception.getCause()).handle((batchExc) -> LOG.error(
                        String.format("failed to deliver %d messages", batchExc.size),
                        batchExc.getCause()));
                  } else {
                    // log exception specific to this message
                    LOG.error("failed to deliver message", exception.getCause());
                  }
                  consumer.nack();
                }
              })))
          .build();
    }

    /** Run until stopAsync() is called. */
    @Override
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

  public static class Write implements Function<PubsubMessage, CompletableFuture<String>> {

    private final Executor executor;
    private final Function<Publisher.Builder, Publisher.Builder> config;
    private final Function<PubsubMessage, PubsubMessage> compress;
    private final PubsubMessageToTemplatedString topicTemplate;
    private final ConcurrentMap<String, Publisher> publishers = new ConcurrentHashMap<>();

    /** Constructor. */
    public Write(String topicTemplate, Executor executor,
        Function<Publisher.Builder, Publisher.Builder> config,
        Function<PubsubMessage, PubsubMessage> compress) {
      this.executor = executor;
      this.topicTemplate = PubsubMessageToTemplatedString.of(topicTemplate);
      this.config = config;
      this.compress = compress;
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
      final PubsubMessage compressed = compress.apply(message);
      final ApiFuture<String> future = getPublisher(message).publish(compressed);
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
