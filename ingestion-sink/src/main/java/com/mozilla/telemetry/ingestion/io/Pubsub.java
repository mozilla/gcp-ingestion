/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.io;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class Pubsub {

  private Pubsub() {
  }

  public static class Read {

    @VisibleForTesting
    Subscriber subscriber;

    public Read(String subscriptionName, Function<PubsubMessage, CompletableFuture<?>> output,
        Function<Subscriber.Builder, Subscriber.Builder> config) {
      ProjectSubscriptionName subscription = ProjectSubscriptionName.parse(subscriptionName);
      subscriber = config.apply(Subscriber.newBuilder(subscription,
          (message, consumer) -> CompletableFuture.supplyAsync(() -> message)
              .thenComposeAsync(output::apply).whenCompleteAsync((result, exception) -> {
                if (exception == null) {
                  consumer.ack();
                } else {
                  consumer.nack();
                }
              })))
          .build();
    }

    public Read(String subscriptionName, Function<PubsubMessage, CompletableFuture<?>> output) {
      this(subscriptionName, output, b -> b);
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
    private final Publisher publisher;

    public Write(String topic, int executorThreads) {
      try {
        publisher = Publisher.newBuilder(ProjectTopicName.parse(topic))
            .build();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      executor = Executors.newFixedThreadPool(executorThreads);
    }

    @Override
    public CompletableFuture<String> apply(PubsubMessage message) {
      ApiFuture<String> future = publisher.publish(message);
      CompletableFuture<String> result = new CompletableFuture<>();
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
  }
}
