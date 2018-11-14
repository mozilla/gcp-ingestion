/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import static java.util.concurrent.TimeUnit.HOURS;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.options.OutputFileFormat;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import redis.embedded.RedisServer;

public class DeduplicateTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() throws IOException {
    int redisPort = new redis.embedded.ports.EphemeralPortProvider().next();
    // create testing redis server
    final RedisServer redis = RedisServer.builder().port(redisPort).setting("bind 127.0.0.1")
        .build();
    final URI redisUri = URI.create("redis://localhost:" + redisPort);
    redis.start();

    try {
      // Create new PubsubMessage with element as document_id attribute
      final MapElements<String, PubsubMessage> mapStringsToId = MapElements
          .into(new TypeDescriptor<PubsubMessage>() {
          })
          .via(element -> new PubsubMessage(new byte[0], ImmutableMap.of("document_id", element)));

      // Extract document_id attribute from PubsubMessage
      final MapElements<PubsubMessage, String> mapMessagesToId = MapElements
          .into(TypeDescriptors.strings()).via(element -> element.getAttribute("document_id"));

      // Only pass this through MarkAsSeen
      final String seenId = UUID.randomUUID().toString();

      // Pass this through MarkAsSeen then RemoveDuplicates
      final String duplicatedId = UUID.randomUUID().toString();

      // Only pass this through RemoveDuplicates
      final String newId = UUID.randomUUID().toString();

      // mark messages as delivered
      final PCollectionTuple seen = pipeline
          .apply("delivered", Create.of(Arrays.asList(seenId, duplicatedId)))
          .apply("create seen messages", mapStringsToId)
          .apply("record seen ids", Deduplicate.markAsSeen(redisUri, (int) HOURS.toSeconds(24)));

      // errorTag is empty
      PAssert.that(seen.get(Deduplicate.errorTag).apply(OutputFileFormat.json.encode())).empty();

      // mainTag contains seen ids
      final PCollection<String> seenMain = seen.get(Deduplicate.mainTag).apply("get seen ids",
          mapMessagesToId);
      PAssert.that(seenMain).containsInAnyOrder(Arrays.asList(seenId, duplicatedId));

      // run MarkAsSeen
      pipeline.run();

      // deduplicate messages
      final PCollectionTuple output = pipeline
          .apply("ids", Create.of(Arrays.asList(newId, duplicatedId)))
          .apply("create messages", mapStringsToId)
          .apply("deduplicate", Deduplicate.removeDuplicates(redisUri));

      // mainTag contains new ids
      final PCollection<String> main = output.get(Deduplicate.mainTag).apply("get new ids",
          mapMessagesToId);
      PAssert.that(main).containsInAnyOrder(newId);

      // errorTag contains duplicate ids
      final PCollection<String> error = output.get(Deduplicate.errorTag).apply("get duplicate ids",
          mapMessagesToId);
      PAssert.that(error).containsInAnyOrder(duplicatedId);

      // run RemoveDuplicates
      pipeline.run();
    } finally {
      // always stop redis
      redis.stop();
    }
  }

}
