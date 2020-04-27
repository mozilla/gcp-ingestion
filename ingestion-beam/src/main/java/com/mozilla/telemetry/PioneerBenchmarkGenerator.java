package com.mozilla.telemetry;

import com.mozilla.telemetry.options.SinkOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PioneerBenchmarkGenerator {
  // create a temporary directory
  // generate a keyfile metadata
  // encode all of the messages
  // upload into a temporary directory (or named directory)

  public static void main(final String[] args) {
    run(args);
  }

  public interface PioneerBenchmarkOptions extends SinkOptions {
  }

  public static PipelineResult run(final String[] args) {
    PioneerBenchmarkOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(PioneerBenchmarkOptions.class);

    // parsed sink options
    SinkOptions.Parsed sinkOptions = SinkOptions.parseSinkOptions(options);

    Pipeline p = Pipeline.create(sinkOptions);

    p.apply(options.getInputType().read(sinkOptions)) //
        .apply(options.getOutputType().write(sinkOptions));
    return p.run();

  }

}