package com.mozilla.telemetry;

import com.google.common.collect.ImmutableSet;
import com.mozilla.telemetry.contextualservices.AggregateImpressions;
import com.mozilla.telemetry.contextualservices.ContextualServicesReporterOptions;
import com.mozilla.telemetry.contextualservices.FilterByDocType;
import com.mozilla.telemetry.contextualservices.ParseReportingUrl;
import com.mozilla.telemetry.contextualservices.SendRequest;
import com.mozilla.telemetry.ingestion.core.Constant;
import com.mozilla.telemetry.io.Write;
import com.mozilla.telemetry.transforms.DecompressPayload;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

    PCollection<PubsubMessage> requests = pipeline //
        .apply(options.getInputType().read(options)) //
        .apply(FilterByDocType.of(options.getAllowedDocTypes())) //
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())) //
        .apply(ParseReportingUrl.of(options.getUrlAllowList())) //
        .failuresTo(errorCollections);

    SendRequest sendRequest = SendRequest.of(options.getReportingEnabled());
    Write output = options.getOutputType().write(options);

    Set<String> aggregatedDocTypes = ImmutableSet.of("topsites-impression");

    // Aggregate impressions
    requests
        .apply("FilterAggregatedDocTypes",
            Filter.by((message) -> aggregatedDocTypes
                .contains(message.getAttribute(Constant.Attribute.DOCUMENT_TYPE)))) //
        .apply(AggregateImpressions.of(options.getAggregationWindowSize())) //
        .apply(sendRequest).failuresTo(errorCollections) //
        .apply(output).failuresTo(errorCollections); // todo: delete

    requests
        .apply("FilterUnaggregatedDocTypes",
            Filter.by((message) -> !aggregatedDocTypes
                .contains(message.getAttribute(Constant.Attribute.DOCUMENT_TYPE)))) //
        .apply(sendRequest).failuresTo(errorCollections) //
        .apply(output).failuresTo(errorCollections); // todo: delete

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
