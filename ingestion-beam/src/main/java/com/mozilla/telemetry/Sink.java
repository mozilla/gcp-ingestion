/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.options.ErrorOutputType;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.InputType;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.options.OutputType;
import com.mozilla.telemetry.transforms.CompositeTransform;
import com.mozilla.telemetry.transforms.MapElementsWithErrors.ToPubsubMessageFrom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;

public class Sink {
  /**
   * Options supported by {@link Sink}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions {
    @Description("Type of --input; must be one of [pubsub, file]")
    @Default.Enum("pubsub")
    InputType getInputType();

    void setInputType(InputType value);

    @Description("File format for --inputType=file; must be one of"
        + " json (each line contains payload[String] and attributeMap[String,String]) or"
        + " text (each line is payload)")
    @Default.Enum("json")
    InputFileFormat getInputFileFormat();

    void setInputFileFormat(InputFileFormat value);

    @Description("Type of --output; must be one of [pubsub, file, stdout]")
    @Default.Enum("file")
    OutputType getOutputType();

    void setOutputType(OutputType value);

    @Description("File format for --outputType=file|stdout; must be one of "
        + " json (each line contains payload[String] and attributeMap[String,String]) or"
        + " text (each line is payload)")
    @Default.Enum("json")
    OutputFileFormat getOutputFileFormat();

    void setOutputFileFormat(OutputFileFormat value);

    @Description("Type of --errorOutput; must be one of [pubsub, file]")
    @Default.Enum("pubsub")
    ErrorOutputType getErrorOutputType();

    void setErrorOutputType(ErrorOutputType value);

    @Description("Fixed window duration. Defaults to 10m. "
        + "Allowed formats are: "
        + "Ns (for seconds, example: 5s), "
        + "Nm (for minutes, example: 12m), "
        + "Nh (for hours, example: 2h).")
    @Default.String("10m")
    String getWindowDuration();

    void setWindowDuration(String value);

    /* Note: Dataflow templates accept ValueProvider options at runtime, and
     * other options at creation time. When running without templates specify
     * all options at once.
     */

    @Description("Input to read from (path to file, PubSub subscription, etc.)")
    @Required
    ValueProvider<String> getInput();

    void setInput(ValueProvider<String> value);

    @Description("Output to write to (path to file or directory, Pubsub topic, etc.)")
    @Required
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> value);

    @Description("Error output to write to (path to file or directory, Pubsub topic, etc.)")
    @Required
    ValueProvider<String> getErrorOutput();

    void setErrorOutput(ValueProvider<String> value);
  }

  /**
   * Execute an Apache Beam pipeline.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    // register options class so that `--help=Options` works
    PipelineOptionsFactory.register(Options.class);

    final Options options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(Options.class);

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
