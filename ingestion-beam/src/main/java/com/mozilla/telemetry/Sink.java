/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.transforms.DecompressPayload;
import java.util.ArrayList;
import java.util.List;
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

    // Trailing comments are used below to prevent rewrapping by google-java-format.
    pipeline //
        .apply(options.getInputType().read(options)).errorsTo(errorCollections) //
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())) //
        .apply(options.getOutputType().write(options)).errorsTo(errorCollections);

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
