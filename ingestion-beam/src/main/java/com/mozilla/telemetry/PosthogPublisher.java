package com.mozilla.telemetry;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.posthog.FilterByDocType;
import com.mozilla.telemetry.posthog.ParsePosthogEvents;
import com.mozilla.telemetry.posthog.PosthogEvent;
import com.mozilla.telemetry.posthog.PosthogPublisherOptions;
import com.mozilla.telemetry.posthog.SendPosthogRequest;
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
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;

/**
 * Get event data and publish to Posthog.
 */
public class PosthogPublisher extends Sink {

  public static void main(String[] args) {
    run(args);
  }

  /**
   * Run the Posthog publisher pipeline with command line arguments.
   */
  public static PipelineResult run(String[] args) {
    registerOptions();
    final PosthogPublisherOptions.Parsed options = PosthogPublisherOptions
        .parsePosthogPublisherOptions(PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(PosthogPublisherOptions.class));
    return run(options);
  }

  /**
   * Run the Posthog publisher pipeline.
   */
  public static PipelineResult run(PosthogPublisherOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    PCollection<PubsubMessage> messages = pipeline.apply(options.getInputType().read(options))
        .apply(FilterByDocType.of(options.getAllowedDocTypes(), options.getAllowedNamespaces()));

    if (options.getRandomSampleRatio() != null) {
      final Double ratio = options.getRandomSampleRatio();
      messages.apply("SampleBySampleIdOrRandomNumber", Filter.by(message -> {
        message = PubsubConstraints.ensureNonNull(message);
        String sampleId = message.getAttribute(Attribute.SAMPLE_ID);
        return RandomSampler.filterBySampleIdOrRandomNumber(sampleId, ratio);
      })).apply("RepublishRandomSample", options.getOutputType().write(options));
    }

    final Map<String, String> apiKeys = readPosthogApiKeysFromFile(options.getApiKeys());

    PCollection<KV<String, Iterable<PosthogEvent>>> events = messages
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())
            .withClientCompressionRecorded())
        .apply(ParsePosthogEvents.of(options.getEventsAllowList())).failuresTo(errorCollections) //
        // shard by platform + hash to allow concurrent batches
        .apply("ShardKeys", WithKeys.of((PosthogEvent event) -> { //
          String platform = event.getPlatform();
          int shardId = Math.abs(event.getUserId().hashCode() % 100);
          return platform + "::" + shardId;
        })).setCoder(KvCoder.of(StringUtf8Coder.of(), PosthogEvent.getCoder())) //
        // group into batches per shard
        .apply(GroupIntoBatches.<String, PosthogEvent>ofSize(options.getMaxEventBatchSize()) //
            .withMaxBufferingDuration(Duration.standardSeconds(options.getMaxBufferingDuration()))) //
        // reshuffle to break fusion and allow SendPosthogRequest to scale
        .apply("Reshuffle", Reshuffle.viaRandomKey()) //
        .apply(SendPosthogRequest.of(apiKeys, options.getReportingEnabled(),
            options.getMaxBatchesPerSecond())) //
        .failuresTo(errorCollections);

    PCollectionList.of(errorCollections).apply("FlattenErrorCollections", Flatten.pCollections())
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)).output();

    return pipeline.run();
  }

  static Map<String, String> readPosthogApiKeysFromFile(String path) {
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
