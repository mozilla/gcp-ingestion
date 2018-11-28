/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.decoder.DecoderOptions.Parsed;
import com.mozilla.telemetry.rules.RedisServer;
import com.mozilla.telemetry.transforms.ResultWithErrors;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class DeduplicateTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public final RedisServer redis = new RedisServer();

  @Test
  public void testOutput() throws IOException {
    DecoderOptions decoderOptions = pipeline.getOptions().as(DecoderOptions.class);
    decoderOptions.setDeduplicateExpireDuration(StaticValueProvider.of("24h"));
    decoderOptions.setRedisUri(StaticValueProvider.of(redis.uri));
    Parsed parsedOptions = DecoderOptions.parseDecoderOptions(decoderOptions);
    Integer ttlSeconds = parsedOptions.getDeduplicateExpireSeconds().get();
    URI redisUri = parsedOptions.getParsedRedisUri().get();

    // Create new PubsubMessage with element as document_id attribute
    final MapElements<String, PubsubMessage> mapStringsToId = MapElements
        .into(new TypeDescriptor<PubsubMessage>() {
        }).via(element -> new PubsubMessage(new byte[0], ImmutableMap.of("document_id", element)));

    // Extract document_id attribute from PubsubMessage
    final MapElements<PubsubMessage, String> mapMessagesToId = MapElements
        .into(TypeDescriptors.strings()).via(element -> element.getAttribute("document_id"));

    // Only pass this through MarkAsSeen
    final String seenId = UUID.randomUUID().toString();

    // Pass this through MarkAsSeen then RemoveDuplicates
    final String duplicatedId = UUID.randomUUID().toString();

    // Only pass these through RemoveDuplicates
    final String newId = UUID.randomUUID().toString();
    final String invalidId = "foo";

    // mark messages as delivered
    ResultWithErrors<PCollection<PubsubMessage>> seen = pipeline
        .apply("delivered", Create.of(Arrays.asList(seenId, duplicatedId)))
        .apply("create seen messages", mapStringsToId)
        .apply("record seen ids", Deduplicate.markAsSeen(redisUri, ttlSeconds));

    // errors is empty
    PAssert.that(seen.errors()).empty();

    // mainTag contains seen ids
    final PCollection<String> seenMain = seen.output().apply("get seen ids", mapMessagesToId);
    PAssert.that(seenMain).containsInAnyOrder(Arrays.asList(seenId, duplicatedId));

    // run MarkAsSeen
    pipeline.run();

    // deduplicate messages
    ResultWithErrors<PCollection<PubsubMessage>> output = pipeline
        .apply("ids", Create.of(Arrays.asList(newId, duplicatedId, invalidId)))
        .apply("create messages", mapStringsToId)
        .apply("deduplicate", Deduplicate.removeDuplicates(redisUri));

    // mainTag contains new ids
    final PCollection<String> main = output.output().apply("get new ids", mapMessagesToId);
    PAssert.that(main).containsInAnyOrder(newId);

    // errorTag contains duplicate ids
    final PCollection<String> error = output.errors().apply("get duplicate ids", mapMessagesToId);
    PAssert.that(error).containsInAnyOrder(duplicatedId, invalidId);

    // run RemoveDuplicates
    pipeline.run();
  }

}
