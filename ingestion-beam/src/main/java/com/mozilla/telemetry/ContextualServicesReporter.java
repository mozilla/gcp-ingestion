package com.mozilla.telemetry;

import com.mozilla.telemetry.contextualservices.ContextualServicesReporterOptions;
import com.mozilla.telemetry.contextualservices.FilterByDocType;
import com.mozilla.telemetry.contextualservices.ParseReportingUrl;
import com.mozilla.telemetry.contextualservices.SendRequest;
import com.mozilla.telemetry.transforms.DecompressPayload;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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

    pipeline //
        .apply(options.getInputType().read(options)) //
        .apply(FilterByDocType.of(options.getAllowedDocTypes())) //
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())) //
        .apply(ParseReportingUrl.of(options.getUrlAllowList(), options.getCountryIpList(), //
            options.getOsUserAgentList()))
        .failuresTo(errorCollections) //
        .apply(SendRequest.of(options.getReportingEnabled())).failuresTo(errorCollections);
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
