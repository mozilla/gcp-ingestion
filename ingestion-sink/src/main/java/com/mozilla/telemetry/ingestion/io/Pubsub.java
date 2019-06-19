/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.io;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.concurrent.CompletableFuture;
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
}
