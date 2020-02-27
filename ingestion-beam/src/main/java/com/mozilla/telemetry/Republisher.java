package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.Deduplicate;
import com.mozilla.telemetry.republisher.RandomSampler;
import com.mozilla.telemetry.republisher.RepublishPerChannel;
import com.mozilla.telemetry.republisher.RepublishPerDocType;
import com.mozilla.telemetry.republisher.RepublishPerNamespace;
import com.mozilla.telemetry.republisher.RepublisherOptions;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Republisher extends Sink {

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
    final RepublisherOptions.Parsed options = RepublisherOptions.parseRepublisherOptions(
        PipelineOptionsFactory.fromArgs(args).withValidation().as(RepublisherOptions.class));
    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(RepublisherOptions.Parsed options) {
    // We aren't decoding payloads, so no need to re-compress when republishing.
    options.setOutputPubsubCompression(StaticValueProvider.of(Compression.UNCOMPRESSED));

    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    // Trailing comments are used below to prevent re-wrapping by google-java-format.
    PCollection<PubsubMessage> decoded = pipeline //
        .apply(options.getInputType().read(options));

    // Mark messages as seen in Redis.
    decoded //
        .apply("MarkAsSeen", Deduplicate.markAsSeen(options.getParsedRedisUri(),
            options.getDeduplicateExpireSeconds()))
        .failuresTo(errorCollections);

    // Republish debug messages.
    if (options.getEnableDebugDestination()) {
      RepublisherOptions.Parsed opts = options.as(RepublisherOptions.Parsed.class);
      opts.setOutput(options.getDebugDestination());
      decoded //
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
      decoded //
          .apply("SampleBySampleIdOrRandomNumber", Filter.by(message -> {
            message = PubsubConstraints.ensureNonNull(message);
            String sampleId = message.getAttribute("sample_id");
            return RandomSampler.filterBySampleIdOrRandomNumber(sampleId, ratio);
          })).apply("RepublishRandomSample", opts.getOutputType().write(opts));
    }

    // Republish to per-docType destinations.
    if (options.getPerDocTypeEnabledList() != null) {
      decoded.apply(RepublishPerDocType.of(options));
    }

    // Republish to per-namespace destinations.
    if (options.getPerNamespaceDestinations() != null) {
      decoded.apply(RepublishPerNamespace.of(options));
    }

    // Republish to sampled per-channel destinations.
    if (options.getPerChannelSampleRatios() != null) {
      decoded.apply(RepublishPerChannel.of(options));
    }

    // Write error output collections.
    PCollectionList.of(errorCollections) //
        .apply("FlattenErrorCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }
}
