package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.AddMetadata;
import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.decoder.GeoCityLookup;
import com.mozilla.telemetry.decoder.GeoIspLookup;
import com.mozilla.telemetry.decoder.ParsePayload;
import com.mozilla.telemetry.decoder.ParseProxy;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.decoder.ParseUserAgent;
import com.mozilla.telemetry.decoder.rally.DecryptPayloads;
import com.mozilla.telemetry.transforms.DecompressPayload;
import com.mozilla.telemetry.transforms.LimitPayloadSize;
import com.mozilla.telemetry.transforms.NormalizeAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Decoder extends Sink {

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
    final DecoderOptions.Parsed options = DecoderOptions.parseDecoderOptions(
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DecoderOptions.class));
    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(DecoderOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> failureCollections = new ArrayList<>();

    // We wrap pipeline in Optional for more convenience in chaining together transforms.
    Optional.of(pipeline)

        // Input
        .map(p -> p //
            .apply(options.getInputType().read(options)) //
            // We apply ParseProxy and GeoCityLookup and GeoIspLookup first so that IP
            // address is already removed before any message gets routed to error output; see
            // https://github.com/mozilla/gcp-ingestion/issues/1096
            .apply(ParseProxy.of()) //
            .apply(GeoIspLookup.of(options.getGeoIspDatabase())) //
            .apply(GeoCityLookup.of(options.getGeoCityDatabase(), options.getGeoCityFilter())) //
            .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())))

        // URI Parsing
        .map(p -> p //
            .apply("ParseUri", ParseUri.of()).failuresTo(failureCollections))

        // Special case: decryption of Pioneer payloads
        // Both legacy messages sent to the `telemetry.pioneer-study` doctype
        // and the glean.js-styled messages are supported respectively
        .map(p -> options.getPioneerEnabled() ? p //
            .apply("DecryptRallyAndPioneerPayloads",
                DecryptPayloads.of(options.getPioneerMetadataLocation(),
                    options.getSchemasLocation(), options.getPioneerKmsEnabled(),
                    options.getPioneerDecompressPayload())) //
            .failuresTo(failureCollections) : p)

        // Main output
        .map(p -> p //
            // See discussion in https://github.com/mozilla/gcp-ingestion/issues/776
            .apply("LimitPayloadSize", LimitPayloadSize.toMB(8)).failuresTo(failureCollections) //
            .apply("ParsePayload", ParsePayload.of(options.getSchemasLocation())) //
            .failuresTo(failureCollections) //
            .apply(ParseUserAgent.of()) //
            .apply(NormalizeAttributes.of()) //
            .apply("AddMetadata", AddMetadata.of()).failuresTo(failureCollections) //
            .apply(options.getOutputType().write(options)).failuresTo(failureCollections));

    // Error output
    PCollectionList.of(failureCollections) //
        .apply("FlattenFailureCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }
}
