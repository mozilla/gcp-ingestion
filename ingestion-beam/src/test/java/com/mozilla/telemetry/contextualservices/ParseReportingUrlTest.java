package com.mozilla.telemetry.contextualservices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
    ParseReportingUrl parseReportingUrl = ParseReportingUrl.of(URL_ALLOW_LIST);

    pipeline.run();

    List<Set<String>> allowedUrlSets = parseReportingUrl.loadAllowedUrls();

    Set<String> expectedClickUrls = ImmutableSet.of("click.com", "click2.com", "test.com");
    Set<String> expectedImpressionUrls = ImmutableSet.of("impression.com", "test.com");

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
    Assert.assertTrue(
        parseReportingUrl.isUrlValid(new URL("https://test.com/"), "topsites-impression"));
    Assert.assertTrue(parseReportingUrl.isUrlValid(new URL("https://test.com/"), "topsites-click"));
  }

  @Test
  public void testParsedUrlOutput() {
    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression",
        Attribute.USER_AGENT_OS, "Windows");

    ObjectNode basePayload = Json.createObjectNode();
    basePayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    basePayload.put(Attribute.VERSION, "87.0");

    List<PubsubMessage> input = Stream
        .of("https://moz.impression.com/?param=1&id=a", "https://other.com?id=b")
        .map(url -> basePayload.deepCopy().put(Attribute.REPORTING_URL, url))
        .map(payload -> new PubsubMessage(Json.asBytes(payload), attributes))
        .collect(Collectors.toList());

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

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
      Assert.assertTrue(reportingUrl.contains("id=a"));
      Assert.assertTrue(
          reportingUrl.contains(String.format("%s=", ParsedReportingUrl.PARAM_REGION_CODE)));
      Assert.assertTrue(reportingUrl
          .contains(String.format("%s=%s", ParsedReportingUrl.PARAM_OS_FAMILY, "Windows")));
      Assert.assertTrue(reportingUrl
          .contains(String.format("%s=%s", ParsedReportingUrl.PARAM_COUNTRY_CODE, "US")));
      Assert.assertTrue(reportingUrl
          .contains(String.format("%s=%s", ParsedReportingUrl.PARAM_FORM_FACTOR, "desktop")));
      Assert.assertTrue(
          reportingUrl.contains(String.format("%s=&", ParsedReportingUrl.PARAM_DMA_CODE))
              || reportingUrl.endsWith(String.format("%s=", ParsedReportingUrl.PARAM_DMA_CODE)));

      return null;
    });

    pipeline.run();
  }

  @Test
  public void testRequiredParamCheck() {
    Map<String, String> attributesImpression = ImmutableMap.of(Attribute.DOCUMENT_TYPE,
        "topsites-impression", Attribute.USER_AGENT_OS, "Windows");
    Map<String, String> attributesClick = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-click",
        Attribute.USER_AGENT_OS, "Windows", Attribute.USER_AGENT_VERSION, "123");

    ObjectNode basePayload = Json.createObjectNode();
    basePayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    basePayload.put(Attribute.VERSION, "87.0");

    List<PubsubMessage> input = Stream
        .of("https://moz.impression.com/?id=a", "https://moz.impression.com/?")
        .map(url -> basePayload.deepCopy().put(Attribute.REPORTING_URL, url))
        .map(payload -> new PubsubMessage(Json.asBytes(payload), attributesImpression))
        .collect(Collectors.toList());

    input.addAll(Stream
        .of("https://click.com/?ctag=a&version=1&key=b&ci=c",
            "https://click.com/?ctag=a&version=1&key=b", "https://click.com/?ctag=a&version=&ci=c",
            "https://click.com/?ctag=a&key=b&ci=c", "https://click.com/?version=1&key=b&ci=c")
        .map(url -> basePayload.deepCopy().put(Attribute.REPORTING_URL, url))
        .map(payload -> new PubsubMessage(Json.asBytes(payload), attributesClick))
        .collect(Collectors.toList()));

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(5, Iterables.size(messages));

      messages.forEach(message -> {
        Assert.assertEquals(RejectedMessageException.class.getCanonicalName(),
            message.getAttribute("exception_class"));
      });

      return null;
    });

    PAssert.that(result.output()).satisfies(messages -> {
      Assert.assertEquals(2, Iterables.size(messages));
      return null;
    });

    pipeline.run();
  }

  @Test
  public void testDmaCode() {
    ObjectNode inputPayload = Json.createObjectNode();
    inputPayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    inputPayload.put(Attribute.VERSION, "87.0");
    inputPayload.put(Attribute.REPORTING_URL, "https://test.com?id=a&ctag=1&version=1&key=1&ci=1");

    byte[] payloadBytes = Json.asBytes(inputPayload);

    List<PubsubMessage> input = ImmutableList.of(
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression", Attribute.USER_AGENT_OS,
                "Windows", Attribute.GEO_DMA_CODE, "12")),
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression", Attribute.USER_AGENT_OS,
                "Windows")),
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-click", Attribute.USER_AGENT_OS,
                "Windows", Attribute.GEO_DMA_CODE, "34", Attribute.USER_AGENT_VERSION, "87")),
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "quicksuggest-impression",
                Attribute.USER_AGENT_OS, "Windows", Attribute.GEO_DMA_CODE, "56")),
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "quicksuggest-click", Attribute.USER_AGENT_OS,
                "Windows", Attribute.GEO_DMA_CODE, "78", Attribute.USER_AGENT_VERSION, "87")));

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(0, Iterators.size(messages.iterator()));
      return null;
    });

    PAssert.that(result.output()).satisfies(messages -> {
      Assert.assertEquals(Iterables.size(messages), 5);

      messages.forEach(message -> {
        try {
          String reportingUrl = Json.readObjectNode(message.getPayload())
              .get(Attribute.REPORTING_URL).asText();
          String doctype = message.getAttribute(Attribute.DOCUMENT_TYPE);

          if (doctype.equals("topsites-impression")) {
            if (message.getAttribute(Attribute.GEO_DMA_CODE) != null) {
              Assert.assertTrue(reportingUrl
                  .contains(String.format("%s=%s", ParsedReportingUrl.PARAM_DMA_CODE, "12")));
            } else {
              Assert.assertTrue(
                  reportingUrl.contains(String.format("%s=", ParsedReportingUrl.PARAM_DMA_CODE)));
            }
          } else if (doctype.equals("topsites-click")) {
            Assert.assertTrue(reportingUrl
                .contains(String.format("%s=%s", ParsedReportingUrl.PARAM_DMA_CODE, "34")));
          } else {
            Assert.assertFalse(
                reportingUrl.contains(String.format("%s=", ParsedReportingUrl.PARAM_DMA_CODE)));
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });

      return null;
    });

    pipeline.run();
  }
}
