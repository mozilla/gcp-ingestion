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

    Assert.assertTrue(parseReportingUrl.isUrlValid(new URL("http://click.com"), "click"));
    Assert.assertTrue(parseReportingUrl.isUrlValid(new URL("https://click2.com/a?b=c"), "click"));
    Assert.assertTrue(parseReportingUrl.isUrlValid(new URL("http://abc.click.com"), "click"));
    Assert.assertFalse(parseReportingUrl.isUrlValid(new URL("http://abcclick.com"), "click"));
    Assert.assertFalse(parseReportingUrl.isUrlValid(new URL("http://click.com"), "impression"));
    Assert
        .assertTrue(parseReportingUrl.isUrlValid(new URL("https://impression.com/"), "impression"));
    Assert.assertTrue(parseReportingUrl.isUrlValid(new URL("https://test.com/"), "impression"));
    Assert.assertTrue(parseReportingUrl.isUrlValid(new URL("https://test.com/"), "click"));
  }

  @Test
  public void testParsedUrlOutput() {

    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression",
        Attribute.DOCUMENT_NAMESPACE, "contextual-services", Attribute.USER_AGENT_OS, "Windows");

    ObjectNode basePayload = Json.createObjectNode();
    basePayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    basePayload.put(Attribute.VERSION, "87.0");

    List<PubsubMessage> input = Stream
        .of("https://moz.impression.com/?param=1&id=a", "https://other.com?id=b")
        .map(url -> basePayload.deepCopy().put(Attribute.REPORTING_URL, url))
        .map(payload -> new PubsubMessage(Json.asBytes(payload), attributes))
        .collect(Collectors.toList());

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterators.size(messages.iterator()));
      return null;
    });

    PAssert.that(result.output()).satisfies(sponsoredInteractions -> {

      List<SponsoredInteraction> payloads = new ArrayList<>();
      sponsoredInteractions.forEach(payloads::add);

      Assert.assertEquals("1 interaction in output", 1, payloads.size());

      String reportingUrl = payloads.get(0).getReportingUrl();

      Assert.assertTrue("reportingUrl starts with moz.impression.com",
          reportingUrl.startsWith("https://moz.impression.com/?"));
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
        "topsites-impression", Attribute.DOCUMENT_NAMESPACE, "contextual-services",
        Attribute.USER_AGENT_OS, "Windows");
    Map<String, String> attributesClick = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-click",
        Attribute.DOCUMENT_NAMESPACE, "contextual-services", Attribute.USER_AGENT_OS, "Windows",
        Attribute.USER_AGENT_VERSION, "123");

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

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
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

    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder())).satisfies(messages -> {
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
    String contextId = "aaaa-bbb-ccc-000";
    inputPayload.put(Attribute.CONTEXT_ID, contextId);

    byte[] payloadBytes = Json.asBytes(inputPayload);

    List<PubsubMessage> input = ImmutableList.of(
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression",
                Attribute.DOCUMENT_NAMESPACE, "contextual-services", Attribute.USER_AGENT_OS,
                "Windows", Attribute.GEO_DMA_CODE, "12")),
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression",
                Attribute.DOCUMENT_NAMESPACE, "contextual-services", Attribute.USER_AGENT_OS,
                "Linux")),
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-click", Attribute.DOCUMENT_NAMESPACE,
                "contextual-services", Attribute.USER_AGENT_OS, "Windows", Attribute.GEO_DMA_CODE,
                "34", Attribute.USER_AGENT_VERSION, "87")),
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "quicksuggest-impression",
                Attribute.DOCUMENT_NAMESPACE, "contextual-services", Attribute.USER_AGENT_OS,
                "Windows", Attribute.GEO_DMA_CODE, "56")),
        new PubsubMessage(payloadBytes,
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "quicksuggest-click",
                Attribute.DOCUMENT_NAMESPACE, "contextual-services", Attribute.USER_AGENT_OS,
                "Windows", Attribute.GEO_DMA_CODE, "78", Attribute.USER_AGENT_VERSION, "87")));

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    // We expect this test url to fail for suggest impressions and clicks as of
    // https://bugzilla.mozilla.org/show_bug.cgi?id=1738974
    // so we get 2 failures and 3 successful messages.
    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(2, Iterators.size(messages.iterator()));
      return null;
    });

    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder()))
        .satisfies(sponsoredInteractions -> {
          Assert.assertEquals(Iterables.size(sponsoredInteractions), 3);

          sponsoredInteractions.forEach(interaction -> {
            String reportingUrl = interaction.getReportingUrl();
            String doctype = interaction.getDerivedDocumentType();

            if (doctype.equals("topsites-impression")) {
              if (reportingUrl.contains("Windows")) {
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

            Assert.assertEquals("Expect context-id to match", contextId,
                interaction.getContextId());
          });

          return null;
        });

    pipeline.run();
  }

  @Test
  public void testGleanPings() {

    ObjectNode basePayload = Json.createObjectNode();
    basePayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    basePayload.put(Attribute.SUBMISSION_TIMESTAMP, "2022-03-15T16:42:38Z");

    ObjectNode eventObject = Json.createObjectNode();
    eventObject.put("category", "top_sites");
    eventObject.put("name", "contile_click");
    eventObject.put("timestamp", "0");
    basePayload.putArray("events").add(eventObject);

    String expectedReportingUrl = "https://test.com/?id=foo&param=1&ctag=1&version=1&key=2&ci=4";
    String contextId = "aaaaaaaa-cc1d-49db-927d-3ea2fc2ae9c1";
    ObjectNode metricsObject = Json.createObjectNode();
    metricsObject.putObject("url").put("top_sites.contile_reporting_url", expectedReportingUrl);
    metricsObject.putObject("uuid").put("top_sites.context_id", contextId);

    basePayload.set("metrics", metricsObject);

    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression",
        Attribute.DOCUMENT_NAMESPACE, "org-mozilla-fenix", Attribute.USER_AGENT_OS, "Android");
    List<PubsubMessage> input = Stream.of(basePayload)
        .map(payload -> new PubsubMessage(Json.asBytes(payload), attributes))
        .collect(Collectors.toList());

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    PAssert.that("There are zero failures in the pipeline", result.failures())
        .satisfies(messages -> {
          Assert.assertEquals(0, Iterators.size(messages.iterator()));
          return null;
        });

    PAssert.that("There is one result in the output and it matches expectations", result.output())
        .satisfies(sponsoredInteractions -> {

          List<SponsoredInteraction> payloads = new ArrayList<>();
          sponsoredInteractions.forEach(payloads::add);

          Assert.assertEquals("1 interaction in output", 1, payloads.size());

          SponsoredInteraction interaction = payloads.get(0);
          String reportingUrl = interaction.getReportingUrl();

          Assert.assertEquals("expect a click interactionType",
              SponsoredInteraction.INTERACTION_CLICK, interaction.getInteractionType());

          Assert.assertEquals("expect a topsites source", SponsoredInteraction.SOURCE_TOPSITES,
              interaction.getSource());

          Assert.assertEquals("expect a context-id to match", contextId,
              interaction.getContextId());

          Assert.assertTrue("reportingUrl starts with test.com",
              reportingUrl.startsWith("https://test.com"));
          Assert.assertTrue("contains param1", reportingUrl.contains("param=1"));
          Assert.assertTrue("contains id=foo", reportingUrl.contains("id=foo"));
          Assert.assertTrue("contains region code",
              reportingUrl.contains(String.format("%s=", ParsedReportingUrl.PARAM_REGION_CODE)));
          Assert.assertTrue("contains os family", reportingUrl
              .contains(String.format("%s=%s", ParsedReportingUrl.PARAM_OS_FAMILY, "Android")));
          Assert.assertTrue("contains country code", reportingUrl
              .contains(String.format("%s=%s", ParsedReportingUrl.PARAM_COUNTRY_CODE, "US")));
          Assert.assertTrue("contains form factor", reportingUrl
              .contains(String.format("%s=%s", ParsedReportingUrl.PARAM_FORM_FACTOR, "phone")));
          Assert.assertTrue("contains dma code",
              reportingUrl.contains(String.format("%s=&", ParsedReportingUrl.PARAM_DMA_CODE))
                  || reportingUrl
                      .endsWith(String.format("%s=", ParsedReportingUrl.PARAM_DMA_CODE)));

          return null;
        });

    pipeline.run();
  }
}
