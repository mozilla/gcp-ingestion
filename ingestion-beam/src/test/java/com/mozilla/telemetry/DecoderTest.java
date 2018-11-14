/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.mozilla.telemetry.decoder.AddMetadata;
import com.mozilla.telemetry.decoder.Deduplicate;
import com.mozilla.telemetry.decoder.GeoCityLookup;
import com.mozilla.telemetry.decoder.GzipDecompress;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.decoder.ParseUserAgent;
import com.mozilla.telemetry.decoder.ValidateSchema;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
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

public class DecoderTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void gzipDecompress() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        // TODO calculate this compression for the test
        // payload="$(printf test | gzip -c | base64)"
        "{\"payload\":\"H4sIAM1ekFsAAytJLS4BAAx+f9gEAAAA\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":null,\"payload\":\"dGVzdA==\"}");

    final PCollection<String> output = pipeline.apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode()).get(DecodePubsubMessages.mainTag)
        .apply("gzipDecompress", new GzipDecompress())
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void geoCityLookup() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"remote_addr\":\"8.8.8.8\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"x_forwarded_for\":\"8.8.8.8, 63.245.208.195\""
            + ",\"x_pipeline_proxy\":\"\",\"meta\":\"data\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"remote_addr\":\"10.0.0.2\""
            + ",\"x_forwarded_for\":\"192.168.1.2, 63.245.208.195\"" + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"geo_country\":\"US\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"geo_country\":\"US\",\"meta\":\"data\"},\"payload\":\"\"}",
        "{\"attributeMap\":" + "{\"geo_country\":\"US\"" + ",\"geo_city\":\"Sacramento\""
            + ",\"geo_subdivision1\":\"CA\"" + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline.apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode()).get(DecodePubsubMessages.mainTag)
        .apply("geoCityLookup", new GeoCityLookup("GeoLite2-City.mmdb", null))
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();

    final List<MetricResult<Long>> counters = Lists.newArrayList(result.metrics()
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace(GeoCityLookup.Fn.class)).build())
        .getCounters());

    assertEquals(counters.size(), 7);
    counters.forEach(counter -> assertThat(counter.getCommitted(), greaterThan(0L)));
  }

  @Test
  public void cityFilterNotAllowed() {
    final List<String> input = Arrays.asList("{\"attributeMap\":" + "{\"remote_addr\":\"10.0.0.2\""
        + ",\"x_forwarded_for\":\"192.168.1.2, 63.245.208.195\"" + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList("{\"attributeMap\":" + "{\"geo_country\":\"US\""
        + ",\"geo_subdivision1\":\"CA\"" + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline.apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode()).get(DecodePubsubMessages.mainTag)
        .apply("geoCityLookup",
            new GeoCityLookup("GeoLite2-City.mmdb", "src/test/resources/cityFilters/milton.txt"))
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();
  }

  @Test
  public void cityFilterAllowed() {
    final List<String> input = Arrays.asList("{\"attributeMap\":" + "{\"remote_addr\":\"10.0.0.2\""
        + ",\"x_forwarded_for\":\"192.168.1.2, 63.245.208.195\"" + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList("{\"attributeMap\":" + "{\"geo_country\":\"US\""
        + ",\"geo_city\":\"Sacramento\"" + ",\"geo_subdivision1\":\"CA\"" + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline.apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode()).get(DecodePubsubMessages.mainTag)
        .apply("geoCityLookup",
            new GeoCityLookup("GeoLite2-City.mmdb",
                "src/test/resources/cityFilters/sacramento.txt"))
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();
  }

  @Test
  public void parseUserAgent() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"user_agent\":\"\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":"
            + "{\"user_agent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:63.0)"
            + " Gecko/20100101 Firefox/63.0\"" + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":" + "{\"user_agent_browser\":\"Firefox\""
            + ",\"user_agent_version\":\"63.0\"" + ",\"user_agent_os\":\"Macintosh\""
            + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline.apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode()).get(DecodePubsubMessages.mainTag)
        .apply("parseUserAgent", new ParseUserAgent())
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void parseUri() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":" + "{\"uri\":\"/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648"
            + "/main/Firefox/61.0a1/nightly/20180328030202\"" + "},\"payload\":\"\"}",
        "{\"attributeMap\":"
            + "{\"uri\":\"/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049\""
            + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":" + "{\"app_name\":\"Firefox\"" + ",\"app_version\":\"61.0a1\""
            + ",\"app_build_id\":\"20180328030202\"" + ",\"app_update_channel\":\"nightly\""
            + ",\"document_namespace\":\"telemetry\""
            + ",\"document_id\":\"ce39b608-f595-4c69-b6a6-f7a436604648\""
            + ",\"document_type\":\"main\"" + "},\"payload\":\"\"}",
        "{\"attributeMap\":" + "{\"document_namespace\":\"eng-workflow\""
            + ",\"document_version\":\"1\""
            + ",\"document_id\":\"2c3a0767-d84a-4d02-8a92-fa54a3376049\""
            + ",\"document_type\":\"hgpush\"" + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline.apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode()).get(DecodePubsubMessages.mainTag)
        .apply("parseUri", new ParseUri()).get(ParseUri.mainTag)
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void validateSchema() {
    final List<String> input = Arrays.asList("{}", "{\"id\":null}", "[]", "{");
    final PCollectionTuple output = pipeline.apply(Create.of(input))
        .apply("decodeText", InputFileFormat.text.decode()).get(DecodePubsubMessages.mainTag)
        .apply("addAttributes", MapElements.into(new TypeDescriptor<PubsubMessage>() {
        }).via(element -> new PubsubMessage(element.getPayload(), ImmutableMap
            .of("document_namespace", "test", "document_type", "test", "document_version", "1"))))
        .apply("validateSchema", new ValidateSchema());

    final List<String> expectedMain = Arrays.asList("{}", "{\"id\":null}");
    final PCollection<String> main = output.get(ValidateSchema.mainTag).apply("encodeTextMain",
        OutputFileFormat.text.encode());
    PAssert.that(main).containsInAnyOrder(expectedMain);

    final List<String> expectedError = Arrays.asList("[]", "{");
    final PCollection<String> error = output.get(ValidateSchema.errorTag).apply("encodeTextError",
        OutputFileFormat.text.encode());
    PAssert.that(error).containsInAnyOrder(expectedError);

    // At time of writing this test, there were 47 schemas to load, but that number will
    // likely increase over time.
    assertThat("Instantiating ValidateSchema caused all schemas to be loaded",
        ValidateSchema.numLoadedSchemas(), greaterThan(40));

    pipeline.run();
  }

  @Test
  public void addMetadata() {
    final List<String> input = Arrays.asList("{}", "{\"id\":null}", "[]", "{");
    final PCollectionTuple output = pipeline.apply(Create.of(input))
        .apply("decodeText", InputFileFormat.text.decode()).get(DecodePubsubMessages.mainTag)
        .apply("addAttributes", MapElements.into(new TypeDescriptor<PubsubMessage>() {
        }).via(element -> new PubsubMessage(element.getPayload(), ImmutableMap.of("meta", "data"))))
        .apply("addMetadata", new AddMetadata());

    final List<String> expectedMain = Arrays.asList("{\"metadata\":{\"meta\":\"data\"}}",
        "{\"metadata\":{\"meta\":\"data\"},\"id\":null}");
    final PCollection<String> main = output.get(AddMetadata.mainTag).apply("encodeTextMain",
        OutputFileFormat.text.encode());
    PAssert.that(main).containsInAnyOrder(expectedMain);

    final List<String> expectedError = Arrays.asList("{", "[]");
    final PCollection<String> error = output.get(AddMetadata.errorTag).apply("encodeTextError",
        OutputFileFormat.text.encode());
    PAssert.that(error).containsInAnyOrder(expectedError);

    pipeline.run();
  }

  @Test
  public void deduplicate() throws IOException {
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
