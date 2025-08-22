package com.mozilla.telemetry;

import com.mozilla.telemetry.republisher.RandomSampler;
import com.mozilla.telemetry.republisher.RepublishPerChannel;
import com.mozilla.telemetry.republisher.RepublishPerDocType;
import com.mozilla.telemetry.republisher.RepublishPerNamespace;
import com.mozilla.telemetry.republisher.RepublisherOptions;
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
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.Filter;
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
    // RepublisherOptions will need to be registered in Sink.java as well.
    // And DecoderOptions should be updated to extend RepublisherOptions.
    final DecoderOptions.Parsed options = DecoderOptions.parseDecoderOptions(
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DecoderOptions.class));
    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}. This assumes that
   * DecoderOptions has been updated to include all options from RepublisherOptions.
   */
  public static PipelineResult run(DecoderOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> failureCollections = new ArrayList<>();

    PCollection<PubsubMessage> decoded = Optional.of(pipeline)

        // Input
        .map(p -> p //
            .apply(options.getInputType().read(options)) //
            // We apply ParseUri without failures here, and add failures later, so that parseProxy
            // can use pipeline metadata to adjust what IP to use for geo lookups.
            .apply("ParseUri", ParseUri.withoutFailures()) //
            // We apply ParseProxy and GeoCityLookup and GeoIspLookup first so that IP
            // address is already removed before any message gets routed to error output; see
            // https://github.com/mozilla/gcp-ingestion/issues/1096
            .apply(ParseProxy.of(options.getSchemasLocation())) //
            .apply(GeoIspLookup.of(options.getGeoIspDatabase())) //
            .apply(GeoCityLookup.of(options.getGeoCityDatabase(), options.getGeoCityFilter())) //
            .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())
                .withClientCompressionRecorded()))

        // Special case: parsing structured telemetry pings submitted from Cloud Logging.
        .map(p -> options.getLogIngestionEnabled() ? p //
            // We first extract IP address from the log entry and remove it from the payload
            // for consistency with the standard flow.
            .apply(ExtractIpFromLogEntry.of()) //
            .apply(GeoIspLookup.of(options.getGeoIspDatabase())) //
            .apply(GeoCityLookup.of(options.getGeoCityDatabase(), options.getGeoCityFilter())) //
            // Now we can parse the log entry and route failures to error output
            .apply(ParseLogEntry.of())//
            .failuresTo(failureCollections) : p)

        // Add parse uri failures separately so that they don't prevent geo lookups
        .map(p -> p //
            .apply("ParseUriAddFailures", ParseUri.addFailures()).failuresTo(failureCollections))
        .get();

    // Main output processing
    PCollection<PubsubMessage> mainOutput = decoded
        // See discussion in https://github.com/mozilla/gcp-ingestion/issues/776
        .apply("LimitPayloadSize", LimitPayloadSize.toMB(8)).failuresTo(failureCollections) //
        .apply("ParsePayload", ParsePayload.of(options.getSchemasLocation())) //
        .failuresTo(failureCollections) //
        .apply(ParseUserAgent.of()) //
        .apply(NormalizeAttributes.of()) //
        .apply(SanitizeAttributes.of(options.getSchemasLocation())) //
        .apply("AddMetadata", AddMetadata.of()).failuresTo(failureCollections);

    // Write main output
    mainOutput.apply(options.getOutputType().write(options)).failuresTo(failureCollections);

    // Republish debug messages.
    if (options.getEnableDebugDestination()) {
      RepublisherOptions.Parsed opts = options.as(RepublisherOptions.Parsed.class);
      opts.setOutput(options.getDebugDestination());
      mainOutput //
          .apply("FilterDebugMessages", Filter.by(message -> {
            message = PubsubConstraints.ensureNonNull(message);
            return message.getAttribute("x_debug_id") != null;
          })) //
          .apply("WriteDebugOutput", opts.getOutputType().write(opts));
    }

    // Republish a random sample.
    if (options.getRandomSampleRatio() != null) {
      final Double ratio = options.getRandomSampleRatio();
      RepublisherOptions.Parsed opts = options.as(RepublisherOptions.Parsed.class);
      opts.setOutput(options.getRandomSampleDestination());
      mainOutput //
          .apply("SampleBySampleIdOrRandomNumber", Filter.by(message -> {
            message = PubsubConstraints.ensureNonNull(message);
            String sampleId = message.getAttribute("sample_id");
            return RandomSampler.filterBySampleIdOrRandomNumber(sampleId, ratio);
          })).apply("RepublishRandomSample", opts.getOutputType().write(opts));
    }

    // Republish to per-docType, per-namespace, and per-channel destinations
    if (options.getPerDocTypeDestinations() != null) {
      mainOutput.apply(RepublishPerDocType.of(options));
    }
    if (options.getPerNamespaceDestinations() != null) {
      mainOutput.apply(RepublishPerNamespace.of(options));
    }
    if (options.getPerChannelSampleRatios() != null) {
      mainOutput.apply(RepublishPerChannel.of(options));
    }

    // Error output
    PCollectionList.of(failureCollections) //
        .apply("FlattenFailureCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }
}
