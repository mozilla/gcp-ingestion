package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class AggregateImpressionsTest {

  private SponsoredInteraction.Builder getTestInteraction() {
    return SponsoredInteraction.builder().setInteractionType("click").setSource("topsite")
        .setFormFactor("phone").setContextId("1");
  }

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBuildAggregateUrl() {
    List<KV<String, Long>> input = ImmutableList.of(KV.of("https://test.com", 4L));

    PCollection<SponsoredInteraction> output = pipeline.apply(Create.of(input))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(5))))
        .apply(ParDo.of(new AggregateImpressions.BuildAggregateUrl()));

    PAssert.that(output).satisfies(messages -> {

      Assert.assertEquals(Iterables.size(messages), 1);

      String aggregatedUrl = Iterables.get(messages, 0).getReportingUrl();
      BuildReportingURL builtURL = new BuildReportingURL(aggregatedUrl);

      Assert.assertTrue(aggregatedUrl.startsWith("https://test.com"));
      Assert.assertEquals(builtURL.getQueryParam("impressions"), "4");
      Assert.assertNotNull(builtURL.getQueryParam("end-timestamp"));
      Assert.assertFalse(builtURL.getQueryParam("end-timestamp").isEmpty());
      Assert.assertNotNull(builtURL.getQueryParam("begin-timestamp"));
      Assert.assertFalse(builtURL.getQueryParam("begin-timestamp").isEmpty());
      Assert.assertNotEquals(builtURL.getQueryParam("begin-timestamp"),
          builtURL.getQueryParam("end-timestamp"));

      return null;
    });

    pipeline.run();
  }

  @Test
  public void testGetAggregationKey() {
    String url = "http://test.com?country-code=US&abc=abc&def=a";
    SponsoredInteraction interaction = getTestInteraction().setReportingUrl(url).build();

    String aggKey = AggregateImpressions.getAggregationKey(interaction);

    // Should return url with sorted query params
    Assert.assertEquals(aggKey, "http://test.com?abc=abc&country-code=US&def=a");
  }

  @Test
  public void testAggregation() {
    SponsoredInteraction.Builder baseInteraction = getTestInteraction();

    String attributesUrl1 = String.format("https://test.com?%s=US&%s=",
        BuildReportingURL.PARAM_COUNTRY_CODE, BuildReportingURL.PARAM_REGION_CODE);
    String attributesUrl2 = String.format("https://test.com?%s=DE&%s=",
        BuildReportingURL.PARAM_COUNTRY_CODE, BuildReportingURL.PARAM_REGION_CODE);

    List<SponsoredInteraction> input = ImmutableList.of(
        baseInteraction.setReportingUrl(attributesUrl1).build(),
        baseInteraction.setReportingUrl(attributesUrl2).build(),
        baseInteraction.setReportingUrl(attributesUrl1).build(),
        baseInteraction.setReportingUrl(attributesUrl1).build(),
        baseInteraction.setReportingUrl(attributesUrl2).build());

    PCollection<SponsoredInteraction> output = pipeline.apply(Create.of(input))
        .apply(AggregateImpressions.of("10m"));

    PAssert.that(output).satisfies(interactions -> {

      Assert.assertEquals(Iterables.size(interactions), 2);

      interactions.forEach(interaction -> {
        String reportingUrl = interaction.getReportingUrl();
        BuildReportingURL builtURL = new BuildReportingURL(reportingUrl);

        String country = builtURL.getQueryParam(BuildReportingURL.PARAM_COUNTRY_CODE);
        if ("US".equals(country)) {
          Assert.assertEquals(builtURL.getQueryParam(BuildReportingURL.PARAM_IMPRESSIONS), "3");
        } else if ("DE".equals(country)) {
          Assert.assertEquals(builtURL.getQueryParam(BuildReportingURL.PARAM_IMPRESSIONS), "2");
        } else {
          throw new IllegalArgumentException("unknown country value");
        }

        // Parameters with no values should still be included
        Assert.assertTrue(
            reportingUrl.contains(String.format("%s=", BuildReportingURL.PARAM_REGION_CODE)));

        long windowSize = Long
            .parseLong(builtURL.getQueryParam(BuildReportingURL.PARAM_TIMESTAMP_END))
            - Long.parseLong(builtURL.getQueryParam(BuildReportingURL.PARAM_TIMESTAMP_BEGIN));
        Assert.assertEquals(windowSize, 600L);
      });

      return null;
    });

    pipeline.run();
  }
}
