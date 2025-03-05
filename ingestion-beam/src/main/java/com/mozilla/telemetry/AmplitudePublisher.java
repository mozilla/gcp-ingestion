package com.mozilla.telemetry;

import com.mozilla.telemetry.amplitude.AmplitudeEvent;
import com.mozilla.telemetry.amplitude.AmplitudePublisherOptions;
import com.mozilla.telemetry.amplitude.FilterByDocType;
import com.mozilla.telemetry.amplitude.ParseAmplitudeEvents;
import com.mozilla.telemetry.amplitude.SendRequest;
import com.mozilla.telemetry.republisher.RandomSampler;
import com.mozilla.telemetry.transforms.DecompressPayload;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;

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

    final Map<String, String> apiKeys = readAmplitudeApiKeysFromFile(options.getApiKeys());

    PCollection<KV<String, Iterable<AmplitudeEvent>>> events = messages
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())
            .withClientCompressionRecorded())
        .apply(ParseAmplitudeEvents.of(options.getEventsAllowList())).failuresTo(errorCollections)
        .apply(WithKeys.of((AmplitudeEvent event) -> event.getPlatform())) //
        .setCoder(KvCoder.of(StringUtf8Coder.of(), AmplitudeEvent.getCoder())) //
        // events from same app can be batched and sent to Amplitude in one request
        .apply(GroupIntoBatches.<String, AmplitudeEvent>ofSize(options.getMaxEventBatchSize()) //
            .withMaxBufferingDuration(Duration.standardSeconds(options.getMaxBufferingDuration())))
        .apply(SendRequest.of(apiKeys, options.getReportingEnabled(), //
            options.getMaxBatchesPerSecond()))
        .failuresTo(errorCollections); //

    // Note that there is no write step here for "successes"
    // since the purpose of this job is sending to the Amplitude API.

    // Write error output collections.
    PCollectionList.of(errorCollections) //
        .apply("FlattenErrorCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }

  /**
   * Reads Amplitude API keys from CSV.
   * Each app has their own API key, which maps to an Amplitude project.
   * Expected format:
   * org-mozilla-ios-firefox,api key
   * org-mozilla-fenix,api key
   */
  static Map<String, String> readAmplitudeApiKeysFromFile(String path) {
    Map<String, String> apiKeys = new HashMap<>();
    try (InputStream inputStream = BeamFileInputStream.open(path);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader)) {

      while (reader.ready()) {
        String line = reader.readLine();

        if (line != null && !line.isEmpty()) {
          String[] data = line.split(",");
          apiKeys.put(data[0], data[1]);
        }
      }
    } catch (IOException e) {
      System.err.println("Exception thrown while fetching " + path);
    }

    return apiKeys;
  }
}
