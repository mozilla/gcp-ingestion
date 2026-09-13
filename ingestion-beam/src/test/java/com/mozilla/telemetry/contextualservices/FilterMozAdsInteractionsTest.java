package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class FilterMozAdsInteractionsTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private static SponsoredInteraction interactionWithUrl(String reportingUrl) {
    return SponsoredInteraction.builder() //
        .setInteractionType(SponsoredInteraction.INTERACTION_IMPRESSION) //
        .setSource(SponsoredInteraction.SOURCE_SUGGEST) //
        .setFormFactor(SponsoredInteraction.FORM_DESKTOP) //
        .setContextId("1") //
        .setReportingUrl(reportingUrl) //
        .build();
  }

  /**
   * Interactions on a Mozilla-operated host are dropped, and partner interactions pass through.
   * Both are present in the same bundle so the filter is exercised on a mixed stream, which is the
   * steady state for this job.
   */
  @Test
  public void testDropsOnlyMozAdsInteractions() {
    String ampClickUrl = "https://bridge.us.admarketplace.net/ctp?version=1&ci=1";
    String ampImpressionUrl = "https://imp.mt48.net/imp?id=1";

    List<SponsoredInteraction> input = ImmutableList.of(
        interactionWithUrl("https://ads.mozilla.org/v1/st?suggestion_id=abc"),
        interactionWithUrl("https://ads.allizom.org/v1/st?suggestion_id=def"),
        interactionWithUrl(ampClickUrl), //
        interactionWithUrl(ampImpressionUrl));

    PCollection<SponsoredInteraction> output = pipeline //
        .apply(Create.of(input).withCoder(SponsoredInteraction.getCoder())) //
        .apply(FilterMozAdsInteractions.of());

    PAssert.that(output).satisfies(interactions -> {
      List<String> urls = StreamSupport.stream(interactions.spliterator(), false)
          .map(SponsoredInteraction::getReportingUrl).sorted().collect(Collectors.toList());

      Assert.assertEquals(ImmutableList.of(ampClickUrl, ampImpressionUrl), urls);
      return null;
    });

    pipeline.run();
  }

  /**
   * A lookalike host is not treated as Mozilla-operated, so it is not silently swallowed here. It
   * would still have to clear the URL allow list in {@link ParseReportingUrl} to be sent anywhere.
   */
  @Test
  public void testDoesNotDropLookalikeHosts() {
    String lookalikeUrl = "https://ads.mozilla.org.example.com/v1/st?suggestion_id=abc";

    PCollection<SponsoredInteraction> output = pipeline //
        .apply(Create.of(ImmutableList.of(interactionWithUrl(lookalikeUrl)))
            .withCoder(SponsoredInteraction.getCoder())) //
        .apply(FilterMozAdsInteractions.of());

    PAssert.that(output).satisfies(interactions -> {
      List<String> urls = StreamSupport.stream(interactions.spliterator(), false)
          .map(SponsoredInteraction::getReportingUrl).collect(Collectors.toList());

      Assert.assertEquals(ImmutableList.of(lookalikeUrl), urls);
      return null;
    });

    pipeline.run();
  }
}
