package com.mozilla.telemetry.contextualservices;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Drop interactions whose reporting URL points at a Mozilla-operated host.
 *
 * <p>MARS's suggest ingestor sets {@code reporting_url} to
 * {@code https://ads.mozilla.org/v1/st?suggestion_id=<id>} for non-AMP sponsored suggestions. That
 * URL is not an endpoint and is never requested: it exists only to carry {@code suggestion_id}
 * through the decoder into the Glean ping tables, where an interaction can be joined against
 * the sponsored suggestions snapshots archive.
 *
 * <p>Reporting is therefore a no-op for these interactions.
 *
 * <p>This runs upstream of every send path, so a non-AMP interaction can never reach an ad partner
 * no matter what {@code AggregateImpressions}, {@code LabelSpikes}, or {@code SendRequest} do with
 * the elements they receive.
 */
public class FilterMozAdsInteractions
    extends PTransform<PCollection<SponsoredInteraction>, PCollection<SponsoredInteraction>> {

  public static FilterMozAdsInteractions of() {
    return new FilterMozAdsInteractions();
  }

  private FilterMozAdsInteractions() {
  }

  @Override
  public PCollection<SponsoredInteraction> expand(PCollection<SponsoredInteraction> input) {
    return input.apply(ParDo.of(new Fn()));
  }

  private static class Fn extends DoFn<SponsoredInteraction, SponsoredInteraction> {

    private final Counter filteredUrlsCounter = Metrics.counter(FilterMozAdsInteractions.class,
        "quick_suggest_moz_ads_filtered_urls");

    @ProcessElement
    public void processElement(@Element SponsoredInteraction interaction,
        OutputReceiver<SponsoredInteraction> out) {
      if (ParseReportingUrl.isMozAdsReportingUrl(interaction.getReportingUrl())) {
        filteredUrlsCounter.inc();
        return; // drop element; nothing to report for a Mozilla-operated reporting URL
      }
      out.output(interaction);
    }
  }
}
