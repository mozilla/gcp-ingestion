/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.concurrent.CompletableFuture;
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

    public Read(String subscriptionName, Function<PubsubMessage, CompletableFuture<?>> output,
        Function<Subscriber.Builder, Subscriber.Builder> config) {
      ProjectSubscriptionName subscription = ProjectSubscriptionName.parse(subscriptionName);
      subscriber = config.apply(Subscriber.newBuilder(subscription,
          (message, consumer) -> CompletableFuture.supplyAsync(() -> message)
              .thenComposeAsync(output::apply).whenCompleteAsync((result, exception) -> {
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
}
