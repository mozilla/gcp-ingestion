package com.mozilla.telemetry;

import com.mozilla.telemetry.amplitude.AmplitudeEvent;
import com.mozilla.telemetry.amplitude.AmplitudePublisherOptions;
import com.mozilla.telemetry.amplitude.FilterByDocType;
import com.mozilla.telemetry.amplitude.ParseAmplitudeEvents;
import com.mozilla.telemetry.amplitude.SendRequest;
import com.mozilla.telemetry.decoder.AddMetadata;
import com.mozilla.telemetry.decoder.ParsePayload;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.decoder.ParseUserAgent;
import com.mozilla.telemetry.decoder.SanitizeAttributes;
import com.mozilla.telemetry.republisher.RandomSampler;
import com.mozilla.telemetry.transforms.DecompressPayload;
import com.mozilla.telemetry.transforms.LimitPayloadSize;
import com.mozilla.telemetry.transforms.NormalizeAttributes;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
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

    PCollection<PubsubMessage> messages = pipeline //
        .apply(options.getInputType().read(options)) //
        .apply(ParseUri.of()).failuresTo(errorCollections) //
        .apply(FilterByDocType.of(options.getAllowedDocTypes(), options.getAllowedNamespaces()));

    // Random sample.
    if (options.getRandomSampleRatio() != null) {
      final Double ratio = options.getRandomSampleRatio();
      messages //
          .apply("SampleBySampleIdOrRandomNumber", Filter.by(message -> {
            message = PubsubConstraints.ensureNonNull(message);
            String sampleId = message.getAttribute("sample_id");
            return RandomSampler.filterBySampleIdOrRandomNumber(sampleId, ratio);
          })).apply("RepublishRandomSample", options.getOutputType().write(options));
    }

    PCollection<AmplitudeEvent> events = messages
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())
            .withClientCompressionRecorded())
        .apply("LimitPayloadSize", LimitPayloadSize.toMB(8)).failuresTo(errorCollections) //
        .apply("ParsePayload", ParsePayload.of(options.getSchemasLocation())) //
        .failuresTo(errorCollections) //
        .apply(ParseUserAgent.of()) //
        .apply(NormalizeAttributes.of()) //
        .apply(SanitizeAttributes.of(options.getSchemasLocation())) //
        .apply("AddMetadata", AddMetadata.of()).failuresTo(errorCollections) //
        .apply(ParseAmplitudeEvents.of(options.getEventsAllowList())).failuresTo(errorCollections)
        .apply(SendRequest.of(options.getReportingEnabled())).failuresTo(errorCollections); //

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
