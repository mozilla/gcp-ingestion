package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.AddMetadata;
import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.decoder.ExtractIpFromLogEntry;
import com.mozilla.telemetry.decoder.GeoCityLookup;
import com.mozilla.telemetry.decoder.GeoIspLookup;
import com.mozilla.telemetry.decoder.ParseLogEntry;
import com.mozilla.telemetry.decoder.ParsePayload;
import com.mozilla.telemetry.decoder.ParseProxy;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.decoder.ParseUserAgent;
import com.mozilla.telemetry.decoder.SanitizeAttributes;
import com.mozilla.telemetry.transforms.DecompressPayload;
import com.mozilla.telemetry.transforms.LimitPayloadSize;
import com.mozilla.telemetry.transforms.NormalizeAttributes;
import java.util.ArrayList;
import java.util.List;
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

    // We apply ParseUri without failures here, and add failures later, so that ParseProxy
    // can use pipeline metadata to adjust what IP to use for geo lookups.
    PCollection<PubsubMessage> messages = pipeline //
        .apply(options.getInputType().read(options)) //
        .apply("ParseUri", ParseUri.withoutFailures());

    // For structured telemetry pings submitted via Cloud Logging, extract the IP from the
    // LogEntry payload into the x_forwarded_for attribute so the geo lookups below can use it.
    if (options.getLogIngestionEnabled()) {
      messages = messages.apply(ExtractIpFromLogEntry.of());
    }

    // We apply ParseProxy, GeoIspLookup, and GeoCityLookup before any potential error
    // routing so that the IP address is scrubbed before any message can be routed to
    // error output; see https://github.com/mozilla/gcp-ingestion/issues/1096
    messages = messages //
        .apply(ParseProxy.of(options.getSchemasLocation())) //
        .apply(GeoIspLookup.of(options.getGeoIspDatabase())) //
        .apply(GeoCityLookup.of(options.getGeoCityDatabase(), options.getGeoCityFilter())) //
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())
            .withClientCompressionRecorded());

    // Parse the LogEntry envelope and route failures to the error output. Runs after the
    // geo block so that failed messages carry geo attributes for parity with the standard flow.
    if (options.getLogIngestionEnabled()) {
      messages = messages.apply(ParseLogEntry.of()).failuresTo(failureCollections);
    }

    // Add ParseUri failures separately so that they don't prevent geo lookups.
    messages = messages //
        .apply("ParseUriAddFailures", ParseUri.addFailures()).failuresTo(failureCollections);

    // Main output
    messages //
        // See discussion in https://github.com/mozilla/gcp-ingestion/issues/776
        .apply("LimitPayloadSize", LimitPayloadSize.toMB(8)).failuresTo(failureCollections) //
        .apply("ParsePayload", ParsePayload.of(options.getSchemasLocation()))
        .failuresTo(failureCollections) //
        .apply(ParseUserAgent.of()) //
        .apply(NormalizeAttributes.of()) //
        .apply(SanitizeAttributes.of(options.getSchemasLocation())) //
        .apply("AddMetadata", AddMetadata.of()).failuresTo(failureCollections) //
        .apply(options.getOutputType().write(options)).failuresTo(failureCollections);

    // Error output
    PCollectionList.of(failureCollections) //
        .apply("FlattenFailureCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }
}
