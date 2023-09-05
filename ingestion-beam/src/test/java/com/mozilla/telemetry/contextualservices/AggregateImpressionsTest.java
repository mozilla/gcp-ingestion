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
        .setFormFactor("phone").setContextId("1").setPosition("3");
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
      BuildReportingUrl builtUrl = new BuildReportingUrl(aggregatedUrl);

      Assert.assertTrue(aggregatedUrl.startsWith("https://test.com"));
      Assert.assertEquals(builtUrl.getQueryParam("impressions"), "4");
      Assert.assertNotNull(builtUrl.getQueryParam("end-timestamp"));
      Assert.assertFalse(builtUrl.getQueryParam("end-timestamp").isEmpty());
      Assert.assertNotNull(builtUrl.getQueryParam("begin-timestamp"));
      Assert.assertFalse(builtUrl.getQueryParam("begin-timestamp").isEmpty());
      Assert.assertNotEquals(builtUrl.getQueryParam("begin-timestamp"),
          builtUrl.getQueryParam("end-timestamp"));

      return null;
    });

    pipeline.run();
  }

  @Test
  public void testGetAggregationKey() {
    String url = "http://test.com?slot-number=3&country-code=US&abc=abc&def=a";
    SponsoredInteraction interaction = getTestInteraction().setReportingUrl(url).build();

    String aggKey = AggregateImpressions.getAggregationKey(interaction);

    // Should return url with sorted query params
    Assert.assertEquals(aggKey, "http://test.com?abc=abc&country-code=US&def=a&slot-number=3");
  }

  @Test
  public void testAggregation() {
    SponsoredInteraction.Builder baseInteraction = getTestInteraction();

    String attributesUrl1 = String.format("https://test.com?%s=US&%s=&%s=1",
        BuildReportingUrl.PARAM_COUNTRY_CODE, BuildReportingUrl.PARAM_REGION_CODE,
        BuildReportingUrl.PARAM_POSITION);
    String attributesUrl2 = String.format("https://test.com?%s=DE&%s=&%s=1",
        BuildReportingUrl.PARAM_COUNTRY_CODE, BuildReportingUrl.PARAM_REGION_CODE,
        BuildReportingUrl.PARAM_POSITION);
    String attributesUrl3 = String.format("https://test.com?%s=DE&%s=&%s=2",
        BuildReportingUrl.PARAM_COUNTRY_CODE, BuildReportingUrl.PARAM_REGION_CODE,
        BuildReportingUrl.PARAM_POSITION);

    List<SponsoredInteraction> input = ImmutableList.of(
        baseInteraction.setReportingUrl(attributesUrl1).build(),
        baseInteraction.setReportingUrl(attributesUrl2).build(),
        baseInteraction.setReportingUrl(attributesUrl1).build(),
        baseInteraction.setReportingUrl(attributesUrl1).build(),
        baseInteraction.setReportingUrl(attributesUrl3).build(),
        baseInteraction.setReportingUrl(attributesUrl3).build(),
        baseInteraction.setReportingUrl(attributesUrl2).build());

    PCollection<SponsoredInteraction> output = pipeline.apply(Create.of(input))
        .apply(AggregateImpressions.of("10m"));

    PAssert.that(output).satisfies(interactions -> {

      Assert.assertEquals(Iterables.size(interactions), 3);

      interactions.forEach(interaction -> {
        String reportingUrl = interaction.getReportingUrl();
        BuildReportingUrl builtUrl = new BuildReportingUrl(reportingUrl);

        String country = builtUrl.getQueryParam(BuildReportingUrl.PARAM_COUNTRY_CODE);
        String position = builtUrl.getQueryParam(BuildReportingUrl.PARAM_POSITION);
        if ("US".equals(country)) {
          Assert.assertEquals(builtUrl.getQueryParam(BuildReportingUrl.PARAM_IMPRESSIONS), "3");
        } else if ("DE".equals(country) && "1".equals(position)) {
          Assert.assertEquals(builtUrl.getQueryParam(BuildReportingUrl.PARAM_IMPRESSIONS), "2");
        } else if ("DE".equals(country) && "2".equals(position)) {
          Assert.assertEquals(builtUrl.getQueryParam(BuildReportingUrl.PARAM_IMPRESSIONS), "2");
        } else {
          throw new IllegalArgumentException("unknown value in reporting url parameters");
        }

        // Parameters with no values should still be included
        Assert.assertTrue(
            reportingUrl.contains(String.format("%s=", BuildReportingUrl.PARAM_REGION_CODE)));

        long windowSize = Long
            .parseLong(builtUrl.getQueryParam(BuildReportingUrl.PARAM_TIMESTAMP_END))
            - Long.parseLong(builtUrl.getQueryParam(BuildReportingUrl.PARAM_TIMESTAMP_BEGIN));
        Assert.assertEquals(windowSize, 600L);
      });

      return null;
    });

    pipeline.run();
  }
}
