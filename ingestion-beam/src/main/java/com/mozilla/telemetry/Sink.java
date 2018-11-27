/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.transforms.MapElementsWithErrors.ToPubsubMessageFrom;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;

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

    pipeline.apply("input", options.getInputType().read(options))
        .apply("collect input parsing errors", PTransform.compose((PCollectionTuple input) -> {
          errorCollections.add(input.get(ToPubsubMessageFrom.errorTag));
          return input.get(ToPubsubMessageFrom.mainTag);
        })).apply("write main output", options.getOutputType().write(options))
        .apply("collect output errors", PTransform.compose((PCollection<PubsubMessage> input) -> {
          errorCollections.add(input);
          return input;
        }));

    PCollectionList.of(errorCollections).apply(Flatten.pCollections()).apply("write error output",
        options.getErrorOutputType().write(options));

    pipeline.run();
  }

}
