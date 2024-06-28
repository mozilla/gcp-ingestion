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
import java.util.stream.StreamSupport;
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
  public void testExtractMetrics() {
    final ObjectNode payload = Json.createObjectNode();
    final ObjectNode metrics = payload.putObject("metrics");
    metrics.putObject("url").put("quick_suggest.reporting_url", "http://click.com");
    metrics.putObject("string").put("quick_suggest.ping_type", "quicksuggest-click");
    metrics.putObject("boolean").put("quick_suggest.improve_suggest_experience", false);
    metrics.putObject("quantity").put("quick_suggest.position", 1);
    final String contextId = "aaaa-bbb-ccc-000";
    metrics.putObject("uuid").put("quick_suggest.context_id", contextId);
    metrics.putObject("string").put("quick_suggest.advertiser", "newAdvertiser");

    ObjectNode expect = Json.createObjectNode();
    expect.put("reporting_url", "http://click.com");
    expect.put("improve_suggest_experience", false);
    expect.put("position", 1);
    expect.put("context_id", contextId);
    expect.put("advertiser", "newAdvertiser");
    ObjectNode actual = ParseReportingUrl.extractMetrics(ImmutableList.of("quick_suggest"),
        payload);
    if (!expect.equals(actual)) {
      System.err.println(Json.asString(actual));
      System.err.println(Json.asString(expect));
    }
    assert expect.equals(actual);
  }

  @Test
  public void testParsedUrlOutput() {
    final Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "top-sites",
        Attribute.DOCUMENT_NAMESPACE, "firefox-desktop", Attribute.USER_AGENT_OS, "Windows");

    List<PubsubMessage> input = Stream
        .of("https://moz.impression.com/?param=1&id=a", "https://other.com?id=b").map(url -> {
          final ObjectNode payload = Json.createObjectNode();
          payload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
          payload.put(Attribute.VERSION, "87.0");
          final ObjectNode metrics = payload.putObject("metrics");
          metrics.putObject("quantity").put("top_sites." + Attribute.POSITION, 1);
          metrics.putObject("url").put("top_sites." + Attribute.REPORTING_URL, url);
          metrics.putObject("string").put("top_sites.ping_type", "topsites-impression");
          return payload;
        }).map(payload -> new PubsubMessage(Json.asBytes(payload), attributes))
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
          reportingUrl.contains(String.format("%s=", BuildReportingUrl.PARAM_REGION_CODE)));
      Assert.assertTrue(reportingUrl
          .contains(String.format("%s=%s", BuildReportingUrl.PARAM_OS_FAMILY, "Windows")));
      Assert.assertTrue(reportingUrl
          .contains(String.format("%s=%s", BuildReportingUrl.PARAM_COUNTRY_CODE, "US")));
      Assert.assertTrue(reportingUrl
          .contains(String.format("%s=%s", BuildReportingUrl.PARAM_FORM_FACTOR, "desktop")));
      Assert.assertTrue(
          reportingUrl.contains(String.format("%s=%s", BuildReportingUrl.PARAM_POSITION, "1")));
      Assert
          .assertTrue(reportingUrl.contains(String.format("%s=&", BuildReportingUrl.PARAM_DMA_CODE))
              || reportingUrl.endsWith(String.format("%s=", BuildReportingUrl.PARAM_DMA_CODE)));

      return null;
    });

    pipeline.run();
  }

  @Test
  public void testLegacyParsedUrlOutput() {

    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression",
        Attribute.DOCUMENT_NAMESPACE, "contextual-services", Attribute.USER_AGENT_OS, "Windows");

    ObjectNode basePayload = Json.createObjectNode();
    basePayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    basePayload.put(Attribute.VERSION, "87.0");
    basePayload.put(Attribute.POSITION, "1");

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
          reportingUrl.contains(String.format("%s=", BuildReportingUrl.PARAM_REGION_CODE)));
      Assert.assertTrue(reportingUrl
          .contains(String.format("%s=%s", BuildReportingUrl.PARAM_OS_FAMILY, "Windows")));
      Assert.assertTrue(reportingUrl
          .contains(String.format("%s=%s", BuildReportingUrl.PARAM_COUNTRY_CODE, "US")));
      Assert.assertTrue(reportingUrl
          .contains(String.format("%s=%s", BuildReportingUrl.PARAM_FORM_FACTOR, "desktop")));
      Assert.assertTrue(
          reportingUrl.contains(String.format("%s=%s", BuildReportingUrl.PARAM_POSITION, "1")));
      Assert
          .assertTrue(reportingUrl.contains(String.format("%s=&", BuildReportingUrl.PARAM_DMA_CODE))
              || reportingUrl.endsWith(String.format("%s=", BuildReportingUrl.PARAM_DMA_CODE)));

      return null;
    });

    pipeline.run();
  }

  @Test
  public void testDmaCode() {
    final String contextId = "aaaa-bbb-ccc-000";

    final Map<String, byte[]> payloads = Stream.of("topsites-click", "topsites-impression",
        "quicksuggest-click", "quicksuggest-impression")
        .collect(Collectors.toMap(pingType -> pingType, pingType -> {
          final ObjectNode payload = Json.createObjectNode();
          payload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
          payload.put(Attribute.VERSION, "87.0");
          final ObjectNode metrics = payload.putObject("metrics");
          final String metricPrefix = pingType.startsWith("topsites") ? "top_sites."
              : "quick_suggest.";
          metrics.putObject("url").put(metricPrefix + Attribute.REPORTING_URL,
              "https://test.com?id=a&ctag=1&version=1&key=1&ci=1");
          metrics.putObject("string").put(metricPrefix + "ping_type", pingType);
          metrics.putObject("uuid").put(metricPrefix + Attribute.CONTEXT_ID, contextId);
          return Json.asBytes(payload);
        }));

    List<PubsubMessage> input = ImmutableList.of(
        new PubsubMessage(payloads.get("topsites-impression"),
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "top-sites", Attribute.DOCUMENT_NAMESPACE,
                "firefox-desktop", Attribute.USER_AGENT_OS, "Windows", Attribute.GEO_DMA_CODE,
                "12")),
        new PubsubMessage(payloads.get("topsites-impression"),
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "top-sites", Attribute.DOCUMENT_NAMESPACE,
                "firefox-desktop", Attribute.USER_AGENT_OS, "Linux")),
        new PubsubMessage(payloads.get("topsites-click"),
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "top-sites", Attribute.DOCUMENT_NAMESPACE,
                "firefox-desktop", Attribute.USER_AGENT_OS, "Windows", Attribute.GEO_DMA_CODE, "34",
                Attribute.USER_AGENT_VERSION, "87")),
        new PubsubMessage(payloads.get("quicksuggest-impression"),
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "quick-suggest", Attribute.DOCUMENT_NAMESPACE,
                "firefox-desktop", Attribute.USER_AGENT_OS, "Windows", Attribute.GEO_DMA_CODE,
                "56")),
        new PubsubMessage(payloads.get("quicksuggest-click"),
            ImmutableMap.of(Attribute.DOCUMENT_TYPE, "quick-suggest", Attribute.DOCUMENT_NAMESPACE,
                "firefox-desktop", Attribute.USER_AGENT_OS, "Windows", Attribute.GEO_DMA_CODE, "78",
                Attribute.USER_AGENT_VERSION, "87")));

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(0, Iterators.size(messages.iterator()));
      return null;
    });

    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder()))
        .satisfies(sponsoredInteractions -> {
          Assert.assertEquals(Iterables.size(sponsoredInteractions), 5);

          sponsoredInteractions.forEach(interaction -> {
            String reportingUrl = interaction.getReportingUrl();
            String doctype = interaction.getDerivedDocumentType();

            if (doctype.equals("topsites-impression")) {
              if (reportingUrl.contains("Windows")) {
                Assert.assertTrue(reportingUrl
                    .contains(String.format("%s=%s", BuildReportingUrl.PARAM_DMA_CODE, "12")));
              } else {
                Assert.assertTrue(
                    reportingUrl.contains(String.format("%s=", BuildReportingUrl.PARAM_DMA_CODE)));
              }
            } else if (doctype.equals("topsites-click")) {
              Assert.assertTrue(reportingUrl
                  .contains(String.format("%s=%s", BuildReportingUrl.PARAM_DMA_CODE, "34")));
            } else {
              Assert.assertFalse(
                  reportingUrl.contains(String.format("%s=", BuildReportingUrl.PARAM_DMA_CODE)));
            }

            Assert.assertEquals("Expect context-id to match", contextId,
                interaction.getContextId());
          });

          return null;
        });

    pipeline.run();
  }

  @Test
  public void testLegacyDmaCode() {
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

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(0, Iterators.size(messages.iterator()));
      return null;
    });

    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder()))
        .satisfies(sponsoredInteractions -> {
          Assert.assertEquals(Iterables.size(sponsoredInteractions), 5);

          sponsoredInteractions.forEach(interaction -> {
            String reportingUrl = interaction.getReportingUrl();
            String doctype = interaction.getDerivedDocumentType();

            if (doctype.equals("topsites-impression")) {
              if (reportingUrl.contains("Windows")) {
                Assert.assertTrue(reportingUrl
                    .contains(String.format("%s=%s", BuildReportingUrl.PARAM_DMA_CODE, "12")));
              } else {
                Assert.assertTrue(
                    reportingUrl.contains(String.format("%s=", BuildReportingUrl.PARAM_DMA_CODE)));
              }
            } else if (doctype.equals("topsites-click")) {
              Assert.assertTrue(reportingUrl
                  .contains(String.format("%s=%s", BuildReportingUrl.PARAM_DMA_CODE, "34")));
            } else {
              Assert.assertFalse(
                  reportingUrl.contains(String.format("%s=", BuildReportingUrl.PARAM_DMA_CODE)));
            }

            Assert.assertEquals("Expect context-id to match", contextId,
                interaction.getContextId());
          });

          return null;
        });

    pipeline.run();
  }

  @Test
  public void testCustomDataParam() {
    final String clickUrl = "https://test.com?v=a&adv-id=1&ctag=1&partner=1&version=1&sub2=1&sub1=1&ci"
        + "=1&custom-data=1";
    final String impressionUrl = "https://test.com?v=a&id=a&adv-id=1&ctag=1&partner=1&version=1&sub2=1"
        + "&sub1=1&ci=1&custom-data=1";
    final String contextId = "aaaa-bbb-ccc-000";

    List<PubsubMessage> input = Stream.of("impression", "click")
        .flatMap(interactionType -> Stream.of(true, false).map(scenario -> {
          final ObjectNode payload = Json.createObjectNode();
          payload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
          payload.put(Attribute.VERSION, "116.0");
          final ObjectNode metrics = payload.putObject("metrics");
          final String metricPrefix = "quick_suggest.";
          metrics.putObject("url").put(metricPrefix + Attribute.REPORTING_URL,
              "click".equals(interactionType) ? clickUrl : impressionUrl);
          final ObjectNode stringMetrics = metrics.putObject("string");
          stringMetrics.put(metricPrefix + "ping_type", "quicksuggest-" + interactionType);
          if ("click".equals(interactionType)) {
            if (scenario) {
              stringMetrics.putNull(metricPrefix + "match_type");
            }
          } else {
            stringMetrics.put(metricPrefix + "match_type",
                scenario ? "firefox-suggest" : "best-match");
          }
          stringMetrics.put(metricPrefix + "advertiser", "amazon");
          metrics.putObject("uuid").put(metricPrefix + Attribute.CONTEXT_ID, contextId);
          if (!"click".equals(interactionType) || scenario) {
            metrics.putObject("boolean").put(metricPrefix + "improve_suggest_experience", scenario);
          }
          return new PubsubMessage(Json.asBytes(payload), ImmutableMap.of(Attribute.DOCUMENT_TYPE,
              "quick-suggest", Attribute.DOCUMENT_NAMESPACE, "firefox-desktop"));
        })).collect(Collectors.toList());

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    // We expect 4 successful messages.
    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder()))
        .satisfies(sponsoredInteractions -> {
          Assert.assertEquals(4, Iterables.size(sponsoredInteractions));

          long onlineInteractions = StreamSupport.stream(sponsoredInteractions.spliterator(), false)
              .filter(interaction -> SponsoredInteraction.ONLINE.equals(interaction.getScenario()))
              .count();

          long offlineInteractions = StreamSupport
              .stream(sponsoredInteractions.spliterator(), false)
              .filter(interaction -> SponsoredInteraction.OFFLINE.equals(interaction.getScenario()))
              .count();

          long nullInteractions = StreamSupport.stream(sponsoredInteractions.spliterator(), false)
              .filter(interaction -> interaction.getScenario() == null).count();

          Assert.assertEquals(2, onlineInteractions);
          Assert.assertEquals(1, offlineInteractions);
          Assert.assertEquals(1, nullInteractions);

          sponsoredInteractions.forEach(interaction -> {
            String reportingUrl = interaction.getReportingUrl();
            String doctype = interaction.getDerivedDocumentType();

            if (doctype.equals("quicksuggest-impression")) {
              if (SponsoredInteraction.ONLINE.equals(interaction.getScenario())) {
                Assert.assertTrue(reportingUrl.contains(
                    String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1_online_reg")));
              } else {
                Assert.assertTrue(reportingUrl.contains(
                    String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1_offline_top")));
              }
            } else if (doctype.equals("quicksuggest-click")) {
              if (SponsoredInteraction.ONLINE.equals(interaction.getScenario())) {
                Assert.assertTrue(reportingUrl.contains(
                    String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1_online")));
              } else {
                Assert.assertTrue(reportingUrl
                    .contains(String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1")));
              }
            }

            Assert.assertEquals("Expect context-id to match", contextId,
                interaction.getContextId());
          });

          return null;
        });

    pipeline.run();
  }

  @Test
  public void testCustomDataParamAmazon() {
    final String clickUrl = "https://test.com?v=a&adv-id=1&ctag=1&partner=1&version=1&sub1=1&custom-data=1&ctaid=1&source=1";

    final String impressionUrl = "https://test.com?v=a&id=a&adv-id=1&ctag=1&partner=1&version=1&sub2=1"
        + "&sub1=1&ci=1&custom-data=1";

    final String contextId = "aaaa-bbb-ccc-000";

    List<PubsubMessage> input = Stream.of("impression", "click")
        .flatMap(interactionType -> Stream.of(true, false).map(scenario -> {
          final ObjectNode payload = Json.createObjectNode();
          payload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
          payload.put(Attribute.VERSION, "116.0");
          final ObjectNode metrics = payload.putObject("metrics");
          final String metricPrefix = "quick_suggest.";
          metrics.putObject("url").put(metricPrefix + Attribute.REPORTING_URL,
              "click".equals(interactionType) ? clickUrl : impressionUrl);
          final ObjectNode stringMetrics = metrics.putObject("string");
          stringMetrics.put(metricPrefix + "ping_type", "quicksuggest-" + interactionType);
          if ("click".equals(interactionType)) {
            if (scenario) {
              stringMetrics.putNull(metricPrefix + "match_type");
            }
          } else {
            stringMetrics.put(metricPrefix + "match_type",
                scenario ? "firefox-suggest" : "best-match");
          }
          stringMetrics.put(metricPrefix + "advertiser", "anAdvertiser");
          metrics.putObject("uuid").put(metricPrefix + Attribute.CONTEXT_ID, contextId);
          if (!"click".equals(interactionType) || scenario) {
            metrics.putObject("boolean").put(metricPrefix + "improve_suggest_experience", scenario);
          }
          return new PubsubMessage(Json.asBytes(payload), ImmutableMap.of(Attribute.DOCUMENT_TYPE,
              "quick-suggest", Attribute.DOCUMENT_NAMESPACE, "firefox-desktop"));
        })).collect(Collectors.toList());

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    // We expect 4 successful messages.
    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder()))
        .satisfies(sponsoredInteractions -> {
          Assert.assertEquals(4, Iterables.size(sponsoredInteractions));

          long onlineInteractions = StreamSupport.stream(sponsoredInteractions.spliterator(), false)
              .filter(interaction -> SponsoredInteraction.ONLINE.equals(interaction.getScenario()))
              .count();

          long offlineInteractions = StreamSupport
              .stream(sponsoredInteractions.spliterator(), false)
              .filter(interaction -> SponsoredInteraction.OFFLINE.equals(interaction.getScenario()))
              .count();

          long nullInteractions = StreamSupport.stream(sponsoredInteractions.spliterator(), false)
              .filter(interaction -> interaction.getScenario() == null).count();

          Assert.assertEquals(2, onlineInteractions);
          Assert.assertEquals(1, offlineInteractions);
          Assert.assertEquals(1, nullInteractions);

          sponsoredInteractions.forEach(interaction -> {
            String reportingUrl = interaction.getReportingUrl();
            String doctype = interaction.getDerivedDocumentType();

            if (doctype.equals("quicksuggest-impression")) {
              if (SponsoredInteraction.ONLINE.equals(interaction.getScenario())) {
                Assert.assertTrue(reportingUrl.contains(
                    String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1_online_reg")));
              } else {
                Assert.assertTrue(reportingUrl.contains(
                    String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1_offline_top")));
              }
            } else if (doctype.equals("quicksuggest-click")) {
              if (SponsoredInteraction.ONLINE.equals(interaction.getScenario())) {
                Assert.assertTrue(reportingUrl.contains(
                    String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1_online")));
              } else {
                Assert.assertTrue(reportingUrl
                    .contains(String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1")));
              }
            }

            Assert.assertEquals("Expect context-id to match", contextId,
                interaction.getContextId());
          });

          return null;
        });

    pipeline.run();
  }

  @Test
  public void testLegacyCustomDataParam() {
    String clickUrl = "https://test.com?v=a&adv-id=1&ctag=1&partner=1&version=1&sub2=1&sub1=1&ci"
        + "=1&custom-data=1";
    String impressionUrl = "https://test.com?v=a&id=a&adv-id=1&ctag=1&partner=1&version=1&sub2=1"
        + "&sub1=1&ci=1&custom-data=1";
    ObjectNode inputPayload = Json.createObjectNode();
    inputPayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    inputPayload.put(Attribute.VERSION, "87.0");

    String contextId = "aaaa-bbb-ccc-000";
    inputPayload.put(Attribute.CONTEXT_ID, contextId);

    ObjectNode impressionPayload = inputPayload.put(Attribute.REPORTING_URL, impressionUrl);
    ObjectNode clickPayload = inputPayload.put(Attribute.REPORTING_URL, clickUrl)
        .put(Attribute.ADVERTISER, "amazon");
    ImmutableMap impressionAttributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE,
        "quicksuggest" + "-impression", Attribute.DOCUMENT_NAMESPACE, "contextual-services");
    ImmutableMap clickAttributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "quicksuggest-click",
        Attribute.DOCUMENT_NAMESPACE, "contextual-services");

    List<PubsubMessage> input = ImmutableList.of(
        new PubsubMessage(Json.asBytes(
            impressionPayload.deepCopy().put(Attribute.IMPROVE_SUGGEST_EXPERIENCE_CHECKED, true)
                .put(Attribute.MATCH_TYPE, "firefox-suggest")),
            impressionAttributes),
        new PubsubMessage(Json.asBytes(
            impressionPayload.deepCopy().put(Attribute.IMPROVE_SUGGEST_EXPERIENCE_CHECKED, false)
                .put(Attribute.MATCH_TYPE, "best-match")),
            impressionAttributes),
        new PubsubMessage(Json.asBytes(clickPayload.deepCopy()
            .put(Attribute.IMPROVE_SUGGEST_EXPERIENCE_CHECKED, true).putNull(Attribute.MATCH_TYPE)),
            clickAttributes),
        new PubsubMessage(Json.asBytes(clickPayload), clickAttributes));

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    // We expect 4 successful messages.
    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder()))
        .satisfies(sponsoredInteractions -> {
          Assert.assertEquals(4, Iterables.size(sponsoredInteractions));

          long onlineInteractions = StreamSupport.stream(sponsoredInteractions.spliterator(), false)
              .filter(interaction -> SponsoredInteraction.ONLINE.equals(interaction.getScenario()))
              .count();

          long offlineInteractions = StreamSupport
              .stream(sponsoredInteractions.spliterator(), false)
              .filter(interaction -> SponsoredInteraction.OFFLINE.equals(interaction.getScenario()))
              .count();

          long nullInteractions = StreamSupport.stream(sponsoredInteractions.spliterator(), false)
              .filter(interaction -> interaction.getScenario() == null).count();

          Assert.assertEquals(2, onlineInteractions);
          Assert.assertEquals(1, offlineInteractions);
          Assert.assertEquals(1, nullInteractions);

          sponsoredInteractions.forEach(interaction -> {
            String reportingUrl = interaction.getReportingUrl();
            String doctype = interaction.getDerivedDocumentType();

            if (doctype.equals("quicksuggest-impression")) {
              if (SponsoredInteraction.ONLINE.equals(interaction.getScenario())) {
                Assert.assertTrue(reportingUrl.contains(
                    String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1_online_reg")));
              } else {
                Assert.assertTrue(reportingUrl.contains(
                    String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1_offline_top")));
              }
            } else if (doctype.equals("quicksuggest-click")) {
              if (SponsoredInteraction.ONLINE.equals(interaction.getScenario())) {
                Assert.assertTrue(reportingUrl.contains(
                    String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1_online")));
              } else {
                Assert.assertTrue(reportingUrl
                    .contains(String.format("%s=%s", BuildReportingUrl.PARAM_CUSTOM_DATA, "1")));
              }
            }

            Assert.assertEquals("Expect context-id to match", contextId,
                interaction.getContextId());
          });

          return null;
        });

    pipeline.run();
  }

  @Test
  public void testMobilePingPosition() {
    // GIVEN THIS INPUT
    ObjectNode eventExtraUnderTest = Json.createObjectNode();
    eventExtraUnderTest.put(Attribute.POSITION, "1");

    // Building up the payload required to run the system
    ObjectNode eventObject = Json.createObjectNode();
    eventObject.put("category", "top_sites");
    eventObject.put("name", "contile_impression");
    eventObject.put("timestamp", "0");
    eventObject.set("extra", eventExtraUnderTest);

    ObjectNode basePayload = Json.createObjectNode();
    basePayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    basePayload.put(Attribute.SUBMISSION_TIMESTAMP, "2022-03-15T16:42:38Z");
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

    // WHEN WE PARSE THE REPORTING URL
    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    PAssert.that("There are zero failures in the pipeline", result.failures())
        .satisfies(messages -> {
          Assert.assertEquals(0, Iterators.size(messages.iterator()));
          return null;
        });

    // THEN THE RESULT HAS THE RIGHT POSITION IN IT
    PAssert.that("There is one result in the output and it matches expectations", result.output())
        .satisfies(sponsoredInteractions -> {

          List<SponsoredInteraction> payloads = new ArrayList<>();
          sponsoredInteractions.forEach(payloads::add);

          Assert.assertEquals("1 interaction in output", 1, payloads.size());

          SponsoredInteraction interaction = payloads.get(0);
          String reportingUrl = interaction.getReportingUrl();

          Assert.assertTrue("contains position parameter",
              reportingUrl.contains(String.format("%s=%s", BuildReportingUrl.PARAM_POSITION, "1")));

          return null;
        });

    pipeline.run();
  }

  @Test
  public void testMobileSuggestPings() {
    final String contextId = "aaaaaaaa-cc1d-49db-927d-3ea2fc2ae9c1";

    ObjectNode basePayload = Json.createObjectNode();
    ObjectNode metrics = basePayload.putObject("metrics");
    metrics.putObject("url").put("fx_suggest.reporting_url",
        "https://test.com?v=a&adv-id=1&ctag=1&partner=1&version=1&sub2=1&sub1=1&ci=1&custom-data=1");
    metrics.putObject("string").put("fx_suggest.ping_type", "fxsuggest-impression");
    metrics.putObject("uuid").put("fx_suggest.context_id", contextId);

    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "fx-suggest",
        Attribute.DOCUMENT_NAMESPACE, "org-mozilla-fenix");

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

          Assert.assertEquals("expect an impression interactionType",
              SponsoredInteraction.INTERACTION_IMPRESSION, interaction.getInteractionType());

          Assert.assertEquals("expect a suggest source", SponsoredInteraction.SOURCE_SUGGEST,
              interaction.getSource());

          Assert.assertEquals("expect context id to match", contextId, interaction.getContextId());

          return null;
        });

    pipeline.run();
  }

  @Test
  public void testMobileTopSitesPings() {

    ObjectNode basePayload = Json.createObjectNode();
    basePayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    basePayload.put(Attribute.SUBMISSION_TIMESTAMP, "2022-03-15T16:42:38Z");

    ObjectNode eventExtra = Json.createObjectNode();
    eventExtra.put("position", "1");

    ObjectNode eventObject = Json.createObjectNode();
    eventObject.put("category", "top_sites");
    eventObject.put("name", "contile_click");
    eventObject.put("timestamp", "0");
    eventObject.set("extra", eventExtra);
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
              reportingUrl.contains(String.format("%s=", BuildReportingUrl.PARAM_REGION_CODE)));
          Assert.assertTrue("contains os family", reportingUrl
              .contains(String.format("%s=%s", BuildReportingUrl.PARAM_OS_FAMILY, "Android")));
          Assert.assertTrue("contains country code", reportingUrl
              .contains(String.format("%s=%s", BuildReportingUrl.PARAM_COUNTRY_CODE, "US")));
          Assert.assertTrue("contains form factor", reportingUrl
              .contains(String.format("%s=%s", BuildReportingUrl.PARAM_FORM_FACTOR, "phone")));
          Assert.assertTrue("contains dma code",
              reportingUrl.contains(String.format("%s=&", BuildReportingUrl.PARAM_DMA_CODE))
                  || reportingUrl.endsWith(String.format("%s=", BuildReportingUrl.PARAM_DMA_CODE)));

          return null;
        });

    pipeline.run();
  }

  // @Ignore("Currently fails due to separate issue with user agent parsing")
  @Test
  public void testMobilePingAlternateCategoryName() {

    ObjectNode basePayload = Json.createObjectNode();
    basePayload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    basePayload.put(Attribute.SUBMISSION_TIMESTAMP, "2022-03-15T16:42:38Z");

    ObjectNode eventObject = Json.createObjectNode();
    // On iOS, the category was implemented as "top_site" rather than "top_sites".
    eventObject.put("category", "top_site");
    eventObject.put("name", "contile_click");
    eventObject.put("timestamp", "0");
    basePayload.putArray("events").add(eventObject);

    String expectedReportingUrl = "https://test.com/?id=foo&param=1&ctag=1&version=1&key=2&ci=4";
    String contextId = "aaaaaaaa-cc1d-49db-927d-3ea2fc2ae9c1";
    ObjectNode metricsObject = Json.createObjectNode();
    metricsObject.putObject("url").put("top_site.contile_reporting_url", expectedReportingUrl);
    metricsObject.putObject("uuid").put("top_site.context_id", contextId);

    basePayload.set("metrics", metricsObject);

    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "topsites-impression",
        Attribute.DOCUMENT_NAMESPACE, "org-mozilla-ios-firefox");
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
              reportingUrl.contains(String.format("%s=", BuildReportingUrl.PARAM_REGION_CODE)));
          Assert.assertTrue("contains os family", reportingUrl
              .contains(String.format("%s=%s", BuildReportingUrl.PARAM_OS_FAMILY, "iOS")));
          Assert.assertTrue("contains country code", reportingUrl
              .contains(String.format("%s=%s", BuildReportingUrl.PARAM_COUNTRY_CODE, "US")));
          Assert.assertTrue("contains form factor", reportingUrl
              .contains(String.format("%s=%s", BuildReportingUrl.PARAM_FORM_FACTOR, "phone")));
          Assert.assertTrue("contains dma code",
              reportingUrl.contains(String.format("%s=&", BuildReportingUrl.PARAM_DMA_CODE))
                  || reportingUrl.endsWith(String.format("%s=", BuildReportingUrl.PARAM_DMA_CODE)));

          return null;
        });

    pipeline.run();
  }

  @Test
  public void testSearchWith() {
    final Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "search-with",
        Attribute.DOCUMENT_NAMESPACE, "firefox-desktop", Attribute.USER_AGENT_OS, "Windows");

    List<PubsubMessage> input = Stream
        .of("https://moz.click.com/?param=1&id=a", "https://other.com?id=b").map(url -> {
          final ObjectNode payload = Json.createObjectNode();
          payload.put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
          payload.put(Attribute.VERSION, "87.0");
          final ObjectNode metrics = payload.putObject("metrics");
          metrics.putObject("url").put("search_with." + Attribute.REPORTING_URL, url);
          return payload;
        }).map(payload -> new PubsubMessage(Json.asBytes(payload), attributes))
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
      System.out.println(reportingUrl);

      Assert.assertTrue("reportingUrl starts with moz.impression.com",
          reportingUrl.startsWith("https://moz.click.com/?"));
      Assert.assertTrue(reportingUrl.contains("param=1"));
      Assert.assertTrue(reportingUrl.contains("id=a"));

      return null;
    });

    pipeline.run();
  }
}
