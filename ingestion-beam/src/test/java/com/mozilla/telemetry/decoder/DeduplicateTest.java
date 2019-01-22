/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.decoder.DecoderOptions.Parsed;
import com.mozilla.telemetry.rules.RedisServer;
import com.mozilla.telemetry.transforms.ResultWithErrors;
import java.util.Arrays;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
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
  public void testOutput() {
    DecoderOptions decoderOptions = pipeline.getOptions().as(DecoderOptions.class);
    decoderOptions.setOutputNumShards(1);
    decoderOptions.setErrorOutputNumShards(1);
    decoderOptions.setDeduplicateExpireDuration(pipeline.newProvider("24h"));
    decoderOptions.setRedisUri(pipeline.newProvider(redis.uri));
    Parsed options = DecoderOptions.parseDecoderOptions(decoderOptions);

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
        .apply("create seen messages", mapStringsToId).apply("record seen ids", Deduplicate
            .markAsSeen(options.getParsedRedisUri(), options.getDeduplicateExpireSeconds()));

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
        .apply("deduplicate", Deduplicate.removeDuplicates(options.getParsedRedisUri()));

    // mainTag contains new ids
    final PCollection<String> main = output.output().apply("get new ids", mapMessagesToId);
    PAssert.that(main).containsInAnyOrder(newId);

    // errorTag contains duplicate ids
    final PCollection<String> error = output.errors().apply("get duplicate ids", mapMessagesToId);
    PAssert.that(error).containsInAnyOrder(invalidId);

    // run RemoveDuplicates
    pipeline.run();
  }

}
