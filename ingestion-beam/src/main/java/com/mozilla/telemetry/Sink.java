/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.options.SinkOptions;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
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
    // register options class so that `--help=SinkOptions` works
    PipelineOptionsFactory.register(SinkOptions.class);

    final SinkOptions.Parsed options = SinkOptions.parseSinkOptions(
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SinkOptions.class));
    final Pipeline pipeline = Pipeline.create(options);
    final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

    // Trailing comments are used below to prevent rewrapping by google-java-format.
    pipeline //
        .apply("input", options.getInputType().read(options)) //
        .addErrorCollectionTo(errorCollections).output() //
        .apply("write main output", options.getOutputType().write(options)) //
        .addErrorCollectionTo(errorCollections).output();

    PCollectionList.of(errorCollections).apply(Flatten.pCollections()).apply("write error output",
        options.getErrorOutputType().write(options));

    pipeline.run();
  }

}
