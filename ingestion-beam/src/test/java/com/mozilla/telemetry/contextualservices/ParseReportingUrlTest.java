package com.mozilla.telemetry.contextualservices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.KeyedCounter;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
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

    Set<String> expectedClickUrls = ImmutableSet.of("click.com", "click2.com", "test.com",
        "admarketplace.net", "ampxdirect.com");
    Set<String> expectedImpressionUrls = ImmutableSet.of("impression.com", "test.com",
        "imp.mt48.net");

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
  public void testIsMozAdsReportingUrl() {
    Assert.assertTrue(ParseReportingUrl
        .isMozAdsSuggestPlaceholderToFilter("https://ads.mozilla.org/v1/st?suggestion_id=abc"));
    Assert.assertTrue(ParseReportingUrl
        .isMozAdsSuggestPlaceholderToFilter("https://ads.allizom.org/v1/st?suggestion_id=abc"));

    Assert.assertFalse(ParseReportingUrl
        .isMozAdsSuggestPlaceholderToFilter("https://bridge.us.admarketplace.net/ctp?a=1"));
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://imp.mt48.net/imp?a=1"));
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://mozillacla.ampxdirect.com/"));

    Assert.assertFalse(ParseReportingUrl
        .isMozAdsSuggestPlaceholderToFilter("https://ads.mozilla.org.example.com/?a=1"));
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://evil-ads.mozilla.org.co/"));
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://mozilla.org/?a=1"));
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://x.ads.mozilla.org/"));

    Assert.assertFalse(ParseReportingUrl
        .isMozAdsSuggestPlaceholderToFilter("https://ads.mozilla.org/v1/t?data=abc"));
    Assert.assertFalse(ParseReportingUrl
        .isMozAdsSuggestPlaceholderToFilter("https://ads.allizom.org/v1/t?data=abc"));

    // Only the exact suggest path matches; no prefix, suffix, or nesting.
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://ads.mozilla.org/"));
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://ads.mozilla.org/v1/stx"));
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://ads.mozilla.org/v1/st/"));
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://ads.mozilla.org/v2/st"));
    Assert.assertFalse(
        ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("https://ads.mozilla.org/x/v1/st"));

    Assert.assertFalse(ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter(null));
    Assert.assertFalse(ParseReportingUrl.isMozAdsSuggestPlaceholderToFilter("not a url"));
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
  public void testInternationalSuggest() {
    // Context IDs are not relevant to the behavior; we just use them to simplify the assertions
    final String legacyContextId = "legacy-context-id";
    final String noCountryContextId = "no-country-context-id";

    ObjectNode basePayload = Json.createObjectNode().put(Attribute.NORMALIZED_COUNTRY_CODE, "GB");

    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE, "quick-suggest",
        Attribute.DOCUMENT_NAMESPACE, "firefox-desktop");

    ObjectNode impressionPayload = basePayload.deepCopy();
    ObjectNode impressionMetrics = impressionPayload.putObject("metrics");
    impressionMetrics.putObject("string").put("quick_suggest.ping_type", "quicksuggest-impression")
        .put("quick_suggest.country", "GB");
    impressionMetrics.putObject("url").put("quick_suggest.reporting_url",
        "https://imp.mt48.net/imp?foo=bar");

    ObjectNode clickPayload = basePayload.deepCopy();
    ObjectNode clickMetrics = clickPayload.putObject("metrics");
    clickMetrics.putObject("string").put("quick_suggest.ping_type", "quicksuggest-click")
        .put("quick_suggest.country", "GB");
    clickMetrics.putObject("url").put("quick_suggest.reporting_url",
        "https://bridge.pdx1.admarketplace.net/ctp?foo=bar");

    ObjectNode legacyImpressionPayload = basePayload.deepCopy();
    ObjectNode legacyImpressionMetrics = legacyImpressionPayload.putObject("metrics");
    legacyImpressionMetrics.putObject("uuid").put("quick_suggest.context_id", legacyContextId);
    legacyImpressionMetrics.putObject("string").put("quick_suggest.ping_type",
        "quicksuggest-impression");
    legacyImpressionMetrics.putObject("url").put("quick_suggest.reporting_url",
        "https://imp.mt48.net/static?foo=bar");

    ObjectNode legacyClickPayload = basePayload.deepCopy();
    ObjectNode legacyClickMetrics = legacyClickPayload.putObject("metrics");
    legacyClickMetrics.putObject("uuid").put("quick_suggest.context_id", legacyContextId);
    legacyClickMetrics.putObject("string").put("quick_suggest.ping_type", "quicksuggest-click");
    legacyClickMetrics.putObject("url").put("quick_suggest.reporting_url",
        "https://mozillacla.ampxdirect.com/?foo=bar");

    // Don't set a country code if the client didn't report one; the IP-derived country is not used
    ObjectNode noCountryPayload = basePayload.deepCopy();
    ObjectNode noCountryMetrics = noCountryPayload.putObject("metrics");
    noCountryMetrics.putObject("uuid").put("quick_suggest.context_id", noCountryContextId);
    noCountryMetrics.putObject("string").put("quick_suggest.ping_type", "quicksuggest-impression");
    noCountryMetrics.putObject("url").put("quick_suggest.reporting_url",
        "https://imp.mt48.net/imp?foo=bar");

    List<PubsubMessage> input = ImmutableList.of(
        new PubsubMessage(Json.asBytes(impressionPayload), attributes),
        new PubsubMessage(Json.asBytes(clickPayload), attributes),
        new PubsubMessage(Json.asBytes(legacyImpressionPayload), attributes),
        new PubsubMessage(Json.asBytes(legacyClickPayload), attributes),
        new PubsubMessage(Json.asBytes(noCountryPayload), attributes));

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder()))
        .satisfies(sponsoredInteractions -> {
          Assert.assertEquals(5, Iterables.size(sponsoredInteractions));

          sponsoredInteractions.forEach(interaction -> {
            String reportingUrl = interaction.getReportingUrl();

            boolean shouldAddCountryCode = true;
            boolean shouldAddFormFactor = true;

            if (interaction.getContextId().equals(noCountryContextId)) {
              shouldAddCountryCode = false;
            } else if (interaction.getContextId().equals(legacyContextId)) {
              shouldAddCountryCode = false;
              shouldAddFormFactor = false;
            }

            Assert.assertEquals("country-code in reporting url", shouldAddCountryCode,
                reportingUrl.contains("country-code=GB"));
            Assert.assertEquals("form-factor in reporting url", shouldAddFormFactor,
                reportingUrl.contains("form-factor=desktop"));
          });

          return null;
        });

    pipeline.run();
  }

  /**
   * Suggest pings submitted via OHTTP are geolocated to the OHTTP gateway, so their
   * normalized_country_code is always US. The country-code sent to AMP must be the
   * client-reported country instead. Without one, only pings submitted directly (older clients)
   * fall back to normalized_country_code; OHTTP pings omit the country-code. Pre-Glean
   * contextual-services pings, which have no client-reported country, use normalized_country_code.
   */
  @Test
  public void testClientReportedCountry() {
    // OHTTP submissions have no user agent attributes; direct submissions do
    final Map<String, String> desktopAttributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE,
        "quick-suggest", Attribute.DOCUMENT_NAMESPACE, "firefox-desktop");
    final Map<String, String> desktopDirectAttributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE,
        "quick-suggest", Attribute.DOCUMENT_NAMESPACE, "firefox-desktop",
        Attribute.USER_AGENT_BROWSER, "Firefox", Attribute.USER_AGENT_VERSION, "128");
    final Map<String, String> mobileAttributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE,
        "fx-suggest", Attribute.DOCUMENT_NAMESPACE, "org-mozilla-firefox");
    final Map<String, String> mobileDirectAttributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE,
        "fx-suggest", Attribute.DOCUMENT_NAMESPACE, "org-mozilla-firefox", Attribute.USER_AGENT_OS,
        "Android", Attribute.USER_AGENT_VERSION, "139");
    final Map<String, String> contextualServicesAttributes = ImmutableMap.of(
        Attribute.DOCUMENT_TYPE, "quicksuggest-impression", Attribute.DOCUMENT_NAMESPACE,
        "contextual-services");

    // Each case is identified by a test-only "test-case" param on its reporting URL
    BiFunction<String, String, ObjectNode> glean = (source, testCase) -> {
      ObjectNode payload = Json.createObjectNode();
      ObjectNode metrics = payload.putObject("metrics");
      metrics.putObject("string").put(source + ".ping_type",
          source.equals("fx_suggest") ? "fxsuggest-impression" : "quicksuggest-impression");
      metrics.putObject("url").put(source + ".reporting_url",
          "https://imp.mt48.net/imp?test-case=" + testCase);
      return payload;
    };

    final ObjectNode desktopOhttp = glean.apply("quick_suggest", "desktop-ohttp")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    desktopOhttp.with("metrics").with("string").put("quick_suggest.country", "DE");
    final ObjectNode desktopNoGeo = glean.apply("quick_suggest", "desktop-ohttp-no-geo");
    desktopNoGeo.with("metrics").with("string").put("quick_suggest.country", "FR");
    final ObjectNode desktopBlank = glean.apply("quick_suggest", "desktop-ohttp-blank")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "GB");
    desktopBlank.with("metrics").with("string").put("quick_suggest.country", "");
    final ObjectNode desktopMissing = glean.apply("quick_suggest", "desktop-ohttp-missing")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    // Older clients that predate the client-reported country submit directly
    final ObjectNode desktopDirectMissing = glean.apply("quick_suggest", "desktop-direct-missing")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "GB");
    final ObjectNode mobileOhttp = glean.apply("fx_suggest", "mobile-ohttp")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    mobileOhttp.with("metrics").with("string").put("fx_suggest.country", "IN");
    final ObjectNode mobileMissing = glean.apply("fx_suggest", "mobile-ohttp-missing")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    final ObjectNode mobileDirectMissing = glean.apply("fx_suggest", "mobile-direct-missing")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "GB");
    // The client-reported country must be a two-letter code. Anything else is treated as missing;
    // it is added to the URL without encoding, so "&" could otherwise add query params.
    final ObjectNode desktopExtraParam = glean.apply("quick_suggest", "desktop-ohttp-extra-param")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    desktopExtraParam.with("metrics").with("string").put("quick_suggest.country",
        "DE&form-factor=phone");
    final ObjectNode desktopLowercase = glean.apply("quick_suggest", "desktop-ohttp-lowercase")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    desktopLowercase.with("metrics").with("string").put("quick_suggest.country", "de");
    // The IP country is unrelated to the invalid client value, so the expected value can only
    // come from the IP country
    final ObjectNode desktopDirectInvalid = glean.apply("quick_suggest", "desktop-direct-invalid")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "FR");
    desktopDirectInvalid.with("metrics").with("string").put("quick_suggest.country", "GBR");
    // The client country is preferred over the IP country for direct pings too
    final ObjectNode desktopDirectValid = glean.apply("quick_suggest", "desktop-direct-valid")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "FR");
    desktopDirectValid.with("metrics").with("string").put("quick_suggest.country", "DE");
    // Reporting URLs in the older format never get a country-code, even with a client country
    final ObjectNode desktopStaticUrl = glean.apply("quick_suggest", "desktop-ohttp-static-url")
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "US");
    desktopStaticUrl.with("metrics").with("string").put("quick_suggest.country", "DE");
    desktopStaticUrl.with("metrics").with("url").put("quick_suggest.reporting_url",
        "https://imp.mt48.net/static?test-case=desktop-ohttp-static-url");
    // Pre-Glean contextual-services pings have no client-reported country, even if the payload
    // has a "country" field
    final ObjectNode contextualServices = Json.createObjectNode()
        .put(Attribute.NORMALIZED_COUNTRY_CODE, "IT").put("country", "DE")
        .put(Attribute.REPORTING_URL, "https://imp.mt48.net/imp?test-case=contextual-services");
    final ObjectNode contextualServicesNoGeo = Json.createObjectNode().put(Attribute.REPORTING_URL,
        "https://imp.mt48.net/imp?test-case=contextual-services-no-geo");

    // A null value means no country-code param is sent
    Map<String, String> expectedCountry = new HashMap<>();
    expectedCountry.put("desktop-ohttp", "DE");
    expectedCountry.put("desktop-ohttp-no-geo", "FR");
    expectedCountry.put("desktop-ohttp-blank", null);
    expectedCountry.put("desktop-ohttp-missing", null);
    expectedCountry.put("desktop-direct-missing", "GB");
    expectedCountry.put("mobile-ohttp", "IN");
    expectedCountry.put("mobile-ohttp-missing", null);
    expectedCountry.put("mobile-direct-missing", "GB");
    expectedCountry.put("desktop-ohttp-extra-param", null);
    expectedCountry.put("desktop-ohttp-lowercase", null);
    expectedCountry.put("desktop-direct-invalid", "FR");
    expectedCountry.put("desktop-direct-valid", "DE");
    expectedCountry.put("desktop-ohttp-static-url", null);
    expectedCountry.put("contextual-services", "IT");
    expectedCountry.put("contextual-services-no-geo", null);

    List<PubsubMessage> input = ImmutableList.of(
        new PubsubMessage(Json.asBytes(desktopOhttp), desktopAttributes),
        new PubsubMessage(Json.asBytes(desktopNoGeo), desktopAttributes),
        new PubsubMessage(Json.asBytes(desktopBlank), desktopAttributes),
        new PubsubMessage(Json.asBytes(desktopMissing), desktopAttributes),
        new PubsubMessage(Json.asBytes(desktopDirectMissing), desktopDirectAttributes),
        new PubsubMessage(Json.asBytes(mobileOhttp), mobileAttributes),
        new PubsubMessage(Json.asBytes(mobileMissing), mobileAttributes),
        new PubsubMessage(Json.asBytes(mobileDirectMissing), mobileDirectAttributes),
        new PubsubMessage(Json.asBytes(desktopExtraParam), desktopAttributes),
        new PubsubMessage(Json.asBytes(desktopLowercase), desktopAttributes),
        new PubsubMessage(Json.asBytes(desktopDirectInvalid), desktopDirectAttributes),
        new PubsubMessage(Json.asBytes(desktopDirectValid), desktopDirectAttributes),
        new PubsubMessage(Json.asBytes(desktopStaticUrl), desktopAttributes),
        new PubsubMessage(Json.asBytes(contextualServices), contextualServicesAttributes),
        new PubsubMessage(Json.asBytes(contextualServicesNoGeo), contextualServicesAttributes));

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(0, Iterators.size(messages.iterator()));
      return null;
    });

    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder()))
        .satisfies(sponsoredInteractions -> {
          Map<String, String> actualCountry = new HashMap<>();
          sponsoredInteractions.forEach(interaction -> {
            BuildReportingUrl url = new BuildReportingUrl(interaction.getReportingUrl());
            actualCountry.put(url.getQueryParam("test-case"),
                url.getQueryParam(BuildReportingUrl.PARAM_COUNTRY_CODE));
            if ("desktop-ohttp-extra-param".equals(url.getQueryParam("test-case"))) {
              Assert.assertFalse(
                  "the client country does not add query params to the reporting URL",
                  interaction.getReportingUrl().contains("form-factor=phone"));
            }
          });
          Assert.assertEquals(expectedCountry, actualCountry);
          return null;
        });

    PipelineResult pipelineResult = pipeline.run();

    // Only OHTTP pings whose client country is missing, blank or invalid are counted: direct pings
    // fall back to the IP country, and contextual-services and older-format URLs never use the
    // client country
    Assert.assertEquals(4,
        counterValue(pipelineResult, "firefox_desktop/quick_suggest/missing_client_country"));
    Assert.assertEquals(1,
        counterValue(pipelineResult, "org_mozilla_firefox/fx_suggest/missing_client_country"));
    Assert.assertEquals(0, counterValue(pipelineResult,
        "contextual_services/quicksuggest_impression/missing_client_country"));
  }

  private static long counterValue(PipelineResult result, String name) {
    return StreamSupport.stream(result.metrics()
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(KeyedCounter.class, name)).build())
        .getCounters().spliterator(), false).mapToLong(MetricResult::getCommitted).sum();
  }

  /**
   * A Mozilla-operated reporting URL passes through as a no-op. It carries {@code suggestion_id}
   * into the Glean ping tables rather than calling an endpoint, so unlike the partner hosts in
   * {@link #testInternationalSuggest} it gets no dimensions appended at all and its query string is
   * not re-serialized.
   *
   * <p>{@code ads.mozilla.org} is deliberately absent from the test allow list, so this also verifies
   * the behavior that these URLs are recognized before the allow list is consulted and don't need an
   * entry there.
   */
  @Test
  public void testMozAdsReportingUrlPassesThroughUnmodified() {
    String impressionUrl = "https://ads.mozilla.org/v1/st?suggestion_id="
        + "550e8400-e29b-41d4-a716-446655440000";

    // The URL only ever needs to carry suggestion_id, so the ping carries nothing else.
    ObjectNode payload = Json.createObjectNode();
    payload.put(Attribute.REPORTING_URL, impressionUrl);

    Map<String, String> attributes = ImmutableMap.of(Attribute.DOCUMENT_TYPE,
        "quicksuggest-impression", Attribute.DOCUMENT_NAMESPACE, "contextual-services");

    Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline //
        .apply(Create.of(ImmutableList.of(new PubsubMessage(Json.asBytes(payload), attributes)))) //
        .apply(ParseReportingUrl.of(URL_ALLOW_LIST));

    PAssert.that(result.output().setCoder(SponsoredInteraction.getCoder()))
        .satisfies(interactions -> {
          Assert.assertEquals(1, Iterables.size(interactions));
          Assert.assertEquals(impressionUrl,
              Iterables.getOnlyElement(interactions).getReportingUrl());
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
