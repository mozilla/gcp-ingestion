package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.transforms.DecompressPayload;
import com.mozilla.telemetry.transforms.DeduplicateByDocumentId;
import com.mozilla.telemetry.transforms.PublishBundleMetrics;
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

public class Sink {

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
    registerOptions();
    final SinkOptions.Parsed options = SinkOptions.parseSinkOptions(
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SinkOptions.class));

    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(SinkOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    // We wrap pipeline in Optional for more convenience in chaining together transforms.
    Optional.of(pipeline) //
        .map(p -> p //
            .apply(options.getInputType().read(options)) //
            .apply(DecompressPayload.enabled(options.getDecompressInputPayloads()))
            .apply(PublishBundleMetrics.of())) //
        .map(p -> options.getDeduplicateByDocumentId() ? p.apply(DeduplicateByDocumentId.of()) : p)
        .map(p -> p //
            .apply(options.getOutputType().write(options)).failuresTo(errorCollections));

    PCollectionList.of(errorCollections) //
        .apply("FlattenErrorCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

    return pipeline.run();
  }

  /**
   * Register all options classes so that `--help=DecoderOptions`, etc.
   * works without having to specify additionally specify the appropriate mainClass.
   */
  static void registerOptions() {
    // register options classes so that `--help=SinkOptions`, etc. works
    PipelineOptionsFactory.register(SinkOptions.class);
    PipelineOptionsFactory.register(DecoderOptions.class);
  }

}
