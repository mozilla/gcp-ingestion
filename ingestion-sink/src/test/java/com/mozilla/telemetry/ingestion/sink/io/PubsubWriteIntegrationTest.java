/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.TestWithSinglePubsubTopic;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class PubsubWriteIntegrationTest extends TestWithSinglePubsubTopic {

  @Rule
  public final Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

  @Test
  public void canWriteToStaticDestination() {
    new Pubsub.Write(getTopic(), 1, b -> b).apply(PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8))).build()).join();
    assertEquals(ImmutableList.of("test"),
        pull(1, false).stream().map(m -> m.getData().toStringUtf8()).collect(Collectors.toList()));
  }

  @Test
  public void canWriteToDynamicDestination() {
    new Pubsub.Write("${topic}", 1, b -> b).apply(PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
        .putAttributes("topic", getTopic()).build()).join();
    assertEquals(ImmutableList.of("test"),
        pull(1, false).stream().map(m -> m.getData().toStringUtf8()).collect(Collectors.toList()));
  }
}
