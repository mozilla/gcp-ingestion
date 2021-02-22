package com.mozilla.telemetry;

import com.mozilla.telemetry.contextual_services.ParseReportingUrl;
import com.mozilla.telemetry.contextual_services.ContextualServicesReporterOptions;
import com.mozilla.telemetry.transforms.DecompressPayload;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Get contextual services pings and send requests to URLs in payload
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
        .parseContextualServicesReporterOptions(PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(ContextualServicesReporterOptions.class));
    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(ContextualServicesReporterOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    // We wrap pipeline in Optional for more convenience in chaining together transforms.
    Optional.of(pipeline) // TODO: can remove optional
        .map(p -> p //
            .apply(options.getInputType().read(options)) //
            .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())) //
            .apply(ParseReportingUrl.of(options.getUrlAllowList())).failuresTo(errorCollections) //
            .apply(options.getOutputType().write(options)).failuresTo(errorCollections));

    // Write error output collections.
    PCollectionList.of(errorCollections) //
        .apply("FlattenErrorCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }

}
