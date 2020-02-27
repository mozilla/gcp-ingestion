package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.republisher.RepublisherOptions;
import com.mozilla.telemetry.rules.RedisServer;
import com.mozilla.telemetry.transforms.WithErrors;
import java.util.Arrays;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures.Result;
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
    RepublisherOptions republisherOptions = pipeline.getOptions().as(RepublisherOptions.class);
    republisherOptions.setOutputNumShards(pipeline.newProvider(1));
    republisherOptions.setErrorOutputNumShards(pipeline.newProvider(1));
    republisherOptions.setDeduplicateExpireDuration(pipeline.newProvider("24h"));
    republisherOptions.setRedisUri(pipeline.newProvider(redis.uri));
    RepublisherOptions.Parsed options = RepublisherOptions
        .parseRepublisherOptions(republisherOptions);

    // Create new PubsubMessage with element as document_id attribute
    final MapElements<String, PubsubMessage> mapStringsToId = MapElements
        .into(TypeDescriptor.of(PubsubMessage.class))
        .via(element -> new PubsubMessage(new byte[0], ImmutableMap.of("document_id", element)));

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
    Result<PCollection<PubsubMessage>, PubsubMessage> seen = pipeline
        .apply("delivered", Create.of(Arrays.asList(seenId, duplicatedId)))
        .apply("create seen messages", mapStringsToId).apply("record seen ids", Deduplicate
            .markAsSeen(options.getParsedRedisUri(), options.getDeduplicateExpireSeconds()));

    // errors is empty
    PAssert.that(seen.failures()).empty();

    // mainTag contains seen ids
    final PCollection<String> seenMain = seen.output().apply("get seen ids", mapMessagesToId);
    PAssert.that(seenMain).containsInAnyOrder(Arrays.asList(seenId, duplicatedId));

    // run MarkAsSeen
    pipeline.run();

    // deduplicate messages
    Deduplicate.RemoveDuplicates.Result result = pipeline
        .apply("ids", Create.of(Arrays.asList(newId, duplicatedId, invalidId)))
        .apply("create messages", mapStringsToId)
        .apply("deduplicate", Deduplicate.removeDuplicates(options.getParsedRedisUri()));

    WithErrors.Result<PCollection<PubsubMessage>> ignored = result.ignoreDuplicates();
    WithErrors.Result<PCollection<PubsubMessage>> dupesAsErrors = result
        .sendDuplicateMetadataToErrors();

    // mainTag contains new ids
    final PCollection<String> main = ignored.output().apply("get new ids", mapMessagesToId);
    PAssert.that(main).containsInAnyOrder(newId);

    // errorTag contains only invalid ids when dupes are ignored
    final PCollection<String> errorNoDupes = ignored.errors().apply("get invalid ids",
        mapMessagesToId);
    PAssert.that(errorNoDupes).containsInAnyOrder(invalidId);

    // errorTag contains duplicate ids when dupes are sent to errors
    final PCollection<String> errorWithDupes = dupesAsErrors.errors().apply("get error ids",
        mapMessagesToId);
    PAssert.that(errorWithDupes).containsInAnyOrder(invalidId, duplicatedId);

    // run RemoveDuplicates
    pipeline.run();

    // Check that having a null URI disables deduplication; all messages should pass through to the
    // output collection even if they've been marked as seen in Redis.
    PCollection<String> nonDedupledIds = pipeline
        .apply("ids", Create.of(Arrays.asList(newId, seenId)))
        .apply("create messages", mapStringsToId)
        .apply("no-op deduplicate", Deduplicate.removeDuplicates(StaticValueProvider.of(null)))
        .ignoreDuplicates().output().apply("get non-deduped IDs", mapMessagesToId);
    PAssert.that(nonDedupledIds).containsInAnyOrder(newId, seenId);

    // run RemoveDuplicates
    pipeline.run();
  }

}
