package com.mozilla.telemetry;

import com.google.common.collect.ImmutableSet;
import com.mozilla.telemetry.contextualservices.AggregateImpressions;
import com.mozilla.telemetry.contextualservices.ContextualServicesReporterOptions;
import com.mozilla.telemetry.contextualservices.EmitCounters;
import com.mozilla.telemetry.contextualservices.FilterByDocType;
import com.mozilla.telemetry.contextualservices.LabelClickSpikes;
import com.mozilla.telemetry.contextualservices.ParseReportingUrl;
import com.mozilla.telemetry.contextualservices.SendRequest;
import com.mozilla.telemetry.contextualservices.SponsoredInteraction;
import com.mozilla.telemetry.contextualservices.VerifyMetadata;
import com.mozilla.telemetry.transforms.DecompressPayload;
import com.mozilla.telemetry.util.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * Get contextual services pings and send requests to URLs in payload.
 */
public class ContextualServicesReporter extends Sink {

  /**
   * Execute an Apache Beam pipeline.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    run(args);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   *
   * @param args command line arguments
   */
  public static PipelineResult run(String[] args) {
    registerOptions(); // Defined in Sink.java
    final ContextualServicesReporterOptions.Parsed options = ContextualServicesReporterOptions
        .parseContextualServicesReporterOptions(PipelineOptionsFactory.fromArgs(args)
            .withValidation().as(ContextualServicesReporterOptions.class));
    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(ContextualServicesReporterOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    PCollection<SponsoredInteraction> requests = pipeline //
        .apply(options.getInputType().read(options)) //
        .apply(FilterByDocType.of(options.getAllowedDocTypes(), options.getAllowedNamespaces())) //
        .apply(VerifyMetadata.of()) //
        .failuresTo(errorCollections) //
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())) //
        .apply(ParseReportingUrl.of(options.getUrlAllowList())) //
        .failuresTo(errorCollections) //
        .apply(EmitCounters.of());

    Set<String> individualImpressions = ImmutableSet.of("topsites-impression");
    Set<String> individualClicks = ImmutableSet.of("topsites-click");

    Set<String> unionedDocTypes = Stream
        .concat(individualImpressions.stream(), individualClicks.stream())
        .collect(Collectors.toSet());

    // Aggregate impressions.
    PCollection<SponsoredInteraction> aggregatedImpressions = requests
        .apply("FilterAggregatedDocTypes", Filter.by((interaction) -> individualImpressions //
            .contains(interaction.getDerivedDocumentType())))
        .apply(AggregateImpressions.of(options.getAggregationWindowDuration()));

    // Perform windowed click counting per context_id, adding a click-status to the reporting URL
    // if the count passes a threshold.
    PCollection<SponsoredInteraction> perContextId = requests
        .apply("FilterPerContextIdDocTypes", Filter.by((interaction) -> individualClicks //
            .contains(interaction.getDerivedDocumentType())))
        .apply(LabelClickSpikes.perContextId(options.getClickSpikeThreshold(),
            Time.parseDuration(options.getClickSpikeWindowDuration())));

    PCollection<SponsoredInteraction> unaggregated = requests.apply("FilterUnaggregatedDocTypes",
        Filter.by((interaction) -> !unionedDocTypes //
            .contains(interaction.getDerivedDocumentType())));

    PCollectionList.of(aggregatedImpressions).and(perContextId).and(unaggregated).apply(Flatten.pCollections())
        .apply(SendRequest.of(options.getReportingEnabled(), options.getLogReportingUrls()))
        .failuresTo(errorCollections);

    // Note that there is no write step here for "successes"
    // since the purpose of this job is sending to an external API.

    // Write error output collections.
    PCollectionList.of(errorCollections) //
        .apply("FlattenErrorCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }

}
