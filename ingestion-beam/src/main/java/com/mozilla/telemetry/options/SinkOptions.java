package com.mozilla.telemetry.options;

import com.mozilla.telemetry.Sink;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * DecoderOptions supported by {@link Sink}.
 *
 * <p>Inherits standard configuration options.
 */
public interface SinkOptions extends PipelineOptions {
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
  @Validation.Required
  ValueProvider<String> getInput();
  void setInput(ValueProvider<String> value);

  @Description("Output to write to (path to file or directory, Pubsub topic, etc.)")
  @Validation.Required
  ValueProvider<String> getOutput();
  void setOutput(ValueProvider<String> value);

  @Description("Error output to write to (path to file or directory, Pubsub topic, etc.)")
  @Validation.Required
  ValueProvider<String> getErrorOutput();
  void setErrorOutput(ValueProvider<String> value);
}
