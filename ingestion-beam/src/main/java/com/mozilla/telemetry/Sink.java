/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.transforms.CompositeTransform;
import com.mozilla.telemetry.transforms.MapElementsWithErrors.ToPubsubMessageFrom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;

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
        PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(SinkOptions.class)
    );

    final Pipeline pipeline = Pipeline.create(options);
    final PTransform<PCollection<PubsubMessage>, ? extends POutput> errorOutput =
        options.getErrorOutputType().write(options);

    pipeline
        .apply("input", options.getInputType().read(options))
        .apply("write input parsing errors",
            CompositeTransform.of((PCollectionTuple input) -> {
              input.get(ToPubsubMessageFrom.errorTag).apply(errorOutput);
              return input.get(ToPubsubMessageFrom.mainTag);
            }))
        .apply("write main output", options.getOutputType().write(options))
        .apply("write output errors", errorOutput);

    pipeline.run();
  }

}
