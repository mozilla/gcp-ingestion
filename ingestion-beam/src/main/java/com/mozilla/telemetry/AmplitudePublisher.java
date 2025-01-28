package com.mozilla.telemetry;

import com.mozilla.telemetry.amplitude.AmplitudePublisherOptions;
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
 * Get event data and publish to Amplitude.
 */
public class AmplitudePublisher extends Sink {

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
    final AmplitudePublisherOptions.Parsed options = AmplitudePublisherOptions
        .parseAmplitudePublisherOptions(PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(AmplitudePublisherOptions.class));
    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(AmplitudePublisherOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    // Optional.of(pipeline) //
    // .map(p -> p //
    // .apply(options.getInputType().read(options)) //
    // .apply(ParseUri.of()).failuresTo(errorCollections) //
    // .apply(ParseProxy.of(options.getSchemasLocation())) //
    // .apply(GeoIspLookup.of(options.getGeoIspDatabase())) //
    // .apply(GeoCityLookup.of(options.getGeoCityDatabase(), options.getGeoCityFilter())) //
    // .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())
    // .withClientCompressionRecorded()))
    // .apply("LimitPayloadSize", LimitPayloadSize.toMB(8)).failuresTo(failureCollections) //
    // .apply("ParsePayload", ParsePayload.of(options.getSchemasLocation())) //
    // .failuresTo(failureCollections) //
    // .apply(ParseUserAgent.of()) //
    // .apply(NormalizeAttributes.of()) //
    // .apply(SanitizeAttributes.of(options.getSchemasLocation())) //
    // .apply("AddMetadata", AddMetadata.of()).failuresTo(failureCollections) //
    // .apply(options.getOutputType().write(options)).failuresTo(failureCollections); // todo
    // sendrequest instead

    // Note that there is no write step here for "successes"
    // since the purpose of this job is sending to the Amplitude API.

    // Write error output collections.
    PCollectionList.of(errorCollections) //
        .apply("FlattenErrorCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }
}
