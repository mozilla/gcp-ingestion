/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mozilla.telemetry.Sink;
import com.mozilla.telemetry.util.Time;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.joda.time.Duration;

/**
 * Options supported by {@link Sink}.
 *
 * <p>Inherits standard configuration options.
 */
public interface SinkOptions extends PipelineOptions {

  @Description("Type of source specified by --input")
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

  @Description("Method of writing to BigQuery")
  @Default.Enum("file_loads")
  BigQueryWriteMethod getBqWriteMethod();

  void setBqWriteMethod(BigQueryWriteMethod value);

  @Description("How often to load a batch of files to BigQuery when writing via file_loads")
  @Default.String("5m")
  String getBqTriggeringFrequency();

  void setBqTriggeringFrequency(String value);

  @Description("Number of file shards to stage for BigQuery when writing via file_loads")
  @Default.Integer(100)
  int getBqNumFileShards();

  void setBqNumFileShards(int value);

  @Description("File format for --outputType=file|stdout; must be one of"
      + " json (each line contains payload[String] and attributeMap[String,String]) or"
      + " text (each line is payload)")
  @Default.Enum("json")
  OutputFileFormat getOutputFileFormat();

  void setOutputFileFormat(OutputFileFormat value);

  @Description("Compression format for --outputType=file")
  @Default.Enum("GZIP")
  Compression getOutputFileCompression();

  void setOutputFileCompression(Compression value);

  @Description("Number of output shards for --outputType=file; only relevant for stream"
      + " processing (--inputType=pubsub); in batch mode, the runner determines sharding")
  ValueProvider<Integer> getOutputNumShards();

  void setOutputNumShards(ValueProvider<Integer> value);

  @Description("Type of --errorOutput; must be one of [pubsub, file]")
  @Default.Enum("pubsub")
  ErrorOutputType getErrorOutputType();

  void setErrorOutputType(ErrorOutputType value);

  @Description("Compression format for --errorOutputType=file")
  @Default.Enum("GZIP")
  Compression getErrorOutputFileCompression();

  void setErrorOutputFileCompression(Compression value);

  @Description("Number of output shards for --errorOutputType=file; only relevant for stream"
      + " processing (--inputType=pubsub); in batch mode, the runner determines sharding")
  ValueProvider<Integer> getErrorOutputNumShards();

  void setErrorOutputNumShards(ValueProvider<Integer> value);

  @Hidden
  @Description("If true, include a 'stack_trace' attribute in error output messages;"
      + " this should always be enabled except for specific testing scenarios where we want to "
      + " validate error output without worrying about unstable stack traces")
  @Default.Boolean(true)
  Boolean getIncludeStackTrace();

  void setIncludeStackTrace(Boolean value);

  @Description("Fixed window duration. Allowed formats are:"
      + " Ns (for seconds, example: 5s), Nm (for minutes, example: 12m),"
      + " Nh (for hours, example: 2h).")
  @Default.String("10m")
  String getWindowDuration();

  void setWindowDuration(String value);

  /*
   * Note: Dataflow templates accept ValueProvider options at runtime, and other options at creation
   * time. When running without templates specify all options at once.
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

  @Description("Unless set to false, we will always attempt to decompress gzipped payloads")
  ValueProvider<Boolean> getDecompressInputPayloads();

  void setDecompressInputPayloads(ValueProvider<Boolean> value);

  @Description("Compression format for payloads when --outputType=pubsub; defaults to GZIP")
  ValueProvider<Compression> getOutputPubsubCompression();

  void setOutputPubsubCompression(ValueProvider<Compression> value);

  @Description("Compression format for payloads when --errorOutputType=pubsub; defaults to GZIP")
  ValueProvider<Compression> getErrorOutputPubsubCompression();

  void setErrorOutputPubsubCompression(ValueProvider<Compression> value);

  /*
   * Subinterface and static methods.
   */

  /**
   * A custom {@link PipelineOptions} that includes derived fields.
   *
   * <p>This class should only be instantiated from an existing {@link SinkOptions} instance
   * via the static {@link #parseSinkOptions(SinkOptions)} method.
   * This follows a similar pattern to the Beam Spark runner's {@code SparkContextOptions}
   * which is instantiated from {@code SparkPipelineOptions} and then enriched.
   */
  @Hidden
  interface Parsed extends SinkOptions {

    @JsonIgnore
    Duration getParsedWindowDuration();

    void setParsedWindowDuration(Duration value);

    @JsonIgnore
    Duration getParsedBqTriggeringFrequency();

    void setParsedBqTriggeringFrequency(Duration value);

  }

  /**
   * Return the input {@link SinkOptions} instance promoted to a {@link SinkOptions.Parsed}
   * and with all derived fields set.
   */
  static Parsed parseSinkOptions(SinkOptions options) {
    final Parsed parsed = options.as(Parsed.class);
    enrichSinkOptions(parsed);
    return parsed;
  }

  /**
   * Set all the derived fields of a {@link SinkOptions.Parsed} instance.
   */
  static void enrichSinkOptions(Parsed options) {
    validateSinkOptions(options);
    options.setParsedWindowDuration(Time.parseDuration(options.getWindowDuration()));
    options.setParsedBqTriggeringFrequency(Time.parseDuration(options.getBqTriggeringFrequency()));
    options.setDecompressInputPayloads(
        providerWithDefault(options.getDecompressInputPayloads(), true));
    options.setOutputPubsubCompression(
        providerWithDefault(options.getOutputPubsubCompression(), Compression.GZIP));
    options.setErrorOutputPubsubCompression(
        providerWithDefault(options.getErrorOutputPubsubCompression(), Compression.GZIP));
    options.setOutputNumShards(providerWithDefault(options.getOutputNumShards(), 100));
    options.setErrorOutputNumShards(providerWithDefault(options.getErrorOutputNumShards(), 100));
  }

  /** Detect invalid combinations of parameters and fail fast with helpful error messages. */
  static void validateSinkOptions(SinkOptions options) {
    List<String> errorMessages = new ArrayList<>();
    if (options.getOutputType() == OutputType.bigquery
        && options.getBqWriteMethod() == BigQueryWriteMethod.file_loads
        && options.getInputType() == InputType.pubsub && options.getBqNumFileShards() == 0) {
      errorMessages.add("Missing required parameter:"
          + " --outputNumShards must be set to an explicit non-zero value when"
          + " --outputType=bigquery and --bqWriteMethod=file_loads and the input is unbounded"
          + " (--inputType=pubsub)");
    }
    if (!errorMessages.isEmpty()) {
      throw new IllegalArgumentException(
          "Configuration errors found!\n* " + String.join("\n* ", errorMessages));
    }
  }

  static <T> ValueProvider<T> providerWithDefault(ValueProvider<T> inner, T defaultValue) {
    return NestedValueProvider.of(inner, value -> value == null ? defaultValue : value);
  }
}
