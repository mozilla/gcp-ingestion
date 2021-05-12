package com.mozilla.telemetry.contextualservices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ParseReportingUrlTest {

  private static final String URL_ALLOW_LIST = "src/test/resources/contextualServices/"
      + "urlAllowlist.csv";

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testAllowedUrlsLoadAndFilter() throws IOException {
    ParseReportingUrl parseReportingUrl = ParseReportingUrl
        .of(pipeline.newProvider(URL_ALLOW_LIST));

    pipeline.run();

    List<Set<String>> allowedUrlSets = parseReportingUrl.loadAllowedUrls();

    Set<String> expectedClickUrls = ImmutableSet.of("click.com", "click2.com");
    Set<String> expectedImpressionUrls = ImmutableSet.of("impression.com");

    Assert.assertEquals(expectedClickUrls, allowedUrlSets.get(0));
    Assert.assertEquals(expectedImpressionUrls, allowedUrlSets.get(1));

    Assert.assertTrue(parseReportingUrl.isUrlValid(new URL("http://click.com"), "topsites-click"));
    Assert.assertTrue(
        parseReportingUrl.isUrlValid(new URL("https://click2.com/a?b=c"), "quicksuggest-click"));
    Assert.assertTrue(
        parseReportingUrl.isUrlValid(new URL("http://abc.click.com"), "topsites-click"));
    Assert.assertFalse(
        parseReportingUrl.isUrlValid(new URL("http://abcclick.com"), "topsites-click"));
    Assert.assertFalse(
        parseReportingUrl.isUrlValid(new URL("http://click.com"), "topsites-impression"));
    Assert.assertTrue(
        parseReportingUrl.isUrlValid(new URL("https://impression.com/"), "topsites-impression"));
  }

  @Test
  public void testParsedUrlOutput() {
    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression",
        Attribute.USER_AGENT_OS, "Windows");

    ObjectNode basePayload = Json.createObjectNode();
    basePayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    basePayload.put(Attribute.VERSION, "87.0");

    List<PubsubMessage> input = Stream
        .of("https://moz.impression.com/?param=1", "https://other.com?")
        .map(url -> basePayload.deepCopy().put(Attribute.REPORTING_URL, url))
        .map(payload -> new PubsubMessage(Json.asBytes(payload), attributes))
        .collect(Collectors.toList());

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(pipeline.newProvider(URL_ALLOW_LIST)));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterators.size(messages.iterator()));
      return null;
    });

    PAssert.that(result.output()).satisfies(messages -> {

      List<ObjectNode> payloads = new ArrayList<>();

      messages.forEach(message -> {
        try {
          payloads.add(Json.readObjectNode(message.getPayload()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });

      Assert.assertEquals(1, payloads.size());

      String reportingUrl = payloads.get(0).get(Attribute.REPORTING_URL).asText();

      Assert.assertTrue(reportingUrl.startsWith("https://moz.impression.com/?"));
      Assert.assertTrue(reportingUrl.contains("param=1"));

      return null;
    });

    pipeline.run();
  }
}
