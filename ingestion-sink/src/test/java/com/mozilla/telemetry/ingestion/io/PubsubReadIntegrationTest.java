/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.io;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.common.TestWithPubsubResources;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class PubsubReadIntegrationTest extends TestWithPubsubResources {

  @Test
  public void canReadOneMessage() throws Exception {
    publisher
        .publish(PubsubMessage.newBuilder().setData(ByteString.copyFrom("test".getBytes())).build())
        .get(10, TimeUnit.SECONDS);

    AtomicReference<PubsubMessage> received = new AtomicReference<>();
    AtomicReference<Pubsub.Read> input = new AtomicReference<>();

    input.set(new Pubsub.Read(subscriptionName.toString(),
        // handler
        message -> CompletableFuture.supplyAsync(() -> message) // create a future with message
            .thenAccept(received::set) // add message to received
            .thenRun(() -> input.get().subscriber.stopAsync()), // stop the subscriber
        // config
        builder -> channelProvider.map(channelProvider -> builder
            .setChannelProvider(channelProvider).setCredentialsProvider(noCredentialsProvider))
            .orElse(builder)));

    input.get().run();

    assertEquals("test", new String(received.get().getData().toByteArray()));
  }
}
