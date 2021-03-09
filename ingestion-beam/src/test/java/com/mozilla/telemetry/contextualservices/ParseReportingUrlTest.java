package com.mozilla.telemetry.contextualservices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
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
  private static final String COUNTRY_IP_LIST = "src/test/resources/contextualServices/"
      + "countryIpList.csv";
  private static final String OS_UA_LIST = "src/test/resources/contextualServices/"
      + "osUserAgentList.csv";

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testCountryIpLoadAndParam() throws IOException {
    ParseReportingUrl parseReportingUrl = ParseReportingUrl.of(pipeline.newProvider(URL_ALLOW_LIST),
        pipeline.newProvider(COUNTRY_IP_LIST), pipeline.newProvider(OS_UA_LIST));

    pipeline.run();

    Map<String, String> mapping = parseReportingUrl.loadCountryToIpMapping();

    Map<String, String> expected = ImmutableMap.of("US", "255.255.255.0", "AA", "255.255.255.1");

    Assert.assertEquals(expected, mapping);

    Assert.assertEquals("255.255.255.1", parseReportingUrl.createIpParam("AA"));
    Assert.assertEquals("255.255.255.0", parseReportingUrl.createIpParam("BB"));
  }

  @Test
  public void testOsUserAgentLoadAndParam() throws IOException {
    ParseReportingUrl parseReportingUrl = ParseReportingUrl.of(pipeline.newProvider(URL_ALLOW_LIST),
        pipeline.newProvider(COUNTRY_IP_LIST), pipeline.newProvider(OS_UA_LIST));

    pipeline.run();

    Map<String, String> mapping = parseReportingUrl.loadOsUserAgentMapping();

    // The templates here are invented for testing and include "{0}" to
    // validate injection of OS version.
    Map<String, String> expected = ImmutableMap.of("Windows",
        "W ${client_version} ${client_version}", "Macintosh",
        "M ${client_version} ${client_version}", "Linux", "L ${client_version} ${client_version}");

    Assert.assertEquals(expected, mapping);

    // The output of createUserAgentParam should be URL-encoded, so spaces become "+".
    Assert.assertEquals("W+10+10", parseReportingUrl.createUserAgentParam("Windows", "10"));
    Assert.assertEquals("L+11+11", parseReportingUrl.createUserAgentParam("Linux", "11"));
    Assert.assertEquals("W+12+12", parseReportingUrl.createUserAgentParam("Other", "12"));
  }

  @Test
  public void testAllowedUrlsLoadAndFilter() throws IOException {
    ParseReportingUrl parseReportingUrl = ParseReportingUrl.of(pipeline.newProvider(URL_ALLOW_LIST),
        pipeline.newProvider(COUNTRY_IP_LIST), pipeline.newProvider(OS_UA_LIST));

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
        .apply(ParseReportingUrl.of(pipeline.newProvider(URL_ALLOW_LIST),
            pipeline.newProvider(COUNTRY_IP_LIST), pipeline.newProvider(OS_UA_LIST)));

    PAssert.that(result.failures()).satisfies(messages -> {
      Iterator<PubsubMessage> iterator = messages.iterator();
      int messageCount = 0;

      for (; iterator.hasNext(); iterator.next()) {
        messageCount++;
      }

      Assert.assertEquals(1, messageCount);
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
      Assert.assertTrue(reportingUrl.contains("ua=W+87.0+87.0"));
      Assert.assertTrue(reportingUrl.contains("ip=255.255.255.0"));

      return null;
    });

    pipeline.run();
  }
}
