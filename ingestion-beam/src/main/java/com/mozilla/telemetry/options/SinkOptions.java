package com.mozilla.telemetry.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mozilla.telemetry.Sink;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow.TableRowFormat;
import com.mozilla.telemetry.util.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
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

  @Description("Type of --output; must be one of [pubsub, file, avro, stdout]")
  @Default.Enum("file")
  OutputType getOutputType();

  void setOutputType(OutputType value);

  @Description("Path (local or gs://) to a .tar.gz file containing json schemas and bq schemas"
      + " per docType; the json schemas are used by the Decoder to validate payload structure and"
      + " the bq schemas are used by the BigQuery sink to coerce the payload types to fit the "
      + " destination table; the BigQuery sink will fall back to using the BigQuery API to fetch"
      + " schemas if this option is not configured; the expected format is the output of GitHub's"
      + " archive endpoint for the generated-schemas branch of mozilla-pipeline-schemas:"
      + " https://github.com/"
      + "mozilla-services/mozilla-pipeline-schemas/archive/generated-schemas.tar.gz")
  String getSchemasLocation();

  void setSchemasLocation(String value);

  @Description("Method of reading from BigQuery; the table will either be exported to GCS"
      + " (GA and free, but may take some time to export and may hit quotas) or accessed using the "
      + " BigQuery Storage API (beta and some cost, but faster and no quotas)")
  @Default.Enum("export")
  BigQueryReadMethod getBqReadMethod();

  void setBqReadMethod(BigQueryReadMethod value);

  @Description("When --bqReadMethod=storageapi, all rows of the input table are read by default,"
      + " but this option can take a SQL text filtering statement, similar to a WHERE clause;"
      + " currently, only a single predicate that is a comparison between a column and a constant"
      + " value is supported; a likely choice to limit partitions would be something like"
      + " \"CAST(submission_timestamp AS DATE) BETWEEN '2020-01-10' AND '2020-01-14'\"; see"
      + " https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta1#tablereadoptions")
  String getBqRowRestriction();

  void setBqRowRestriction(String value);

  @Description("When --bqReadMethod=storageapi, all fields of the input table are read by default,"
      + " but this option can take a comma-separated list of field names, in which case only the"
      + " listed fields will be read, saving costs; when reading decoded payload_bytes, none of the"
      + " metadata fields are needed, so setting --bqSelectedFields=payload is recommended")
  List<String> getBqSelectedFields();

  void setBqSelectedFields(List<String> value);

  @Description("Name of time partitioning field of destination tables;"
      + " defaults to submission_timestamp")
  String getBqPartitioningField();

  void setBqPartitioningField(String value);

  @Description("Comma-separated list of clustering fields; defaults to submission_timestamp")
  List<String> getBqClusteringFields();

  void setBqClusteringFields(List<String> value);

  @Description("Method of writing to BigQuery")
  @Default.Enum("file_loads")
  BigQueryWriteMethod getBqWriteMethod();

  void setBqWriteMethod(BigQueryWriteMethod value);

  @Description("How often to load a batch of files to BigQuery when writing via file_loads in"
      + " streaming mode")
  @Default.String("5m")
  String getBqTriggeringFrequency();

  void setBqTriggeringFrequency(String value);

  @Description("Maximum number of bytes to load into a single BigQuery table before rolling"
      + " to an additional partition; this is generally only relevant when writing main_v4 in batch"
      + " mode; we have found empirically that 500 GB per load job is low enough to avoid memory"
      + " exceeded errors in load jobs yet high enough to avoid creating too many intermediate"
      + " tables such that the final copy job fails")
  @Default.Long(500 * (1L << 30))
  Long getBqMaxBytesPerPartition();

  void setBqMaxBytesPerPartition(Long value);

  @Description("Number of file shards to stage for BigQuery when writing via file_loads")
  @Default.Integer(100)
  int getBqNumFileShards();

  void setBqNumFileShards(int value);

  @Description("A comma-separated list of docTypes that should be published to BigQuery via the"
      + " streaming InsertAll endpoint rather than via file loads;"
      + " only relevant if --bqWriteMethod=mixed;"
      + " each docType must be qualified with a namespace like 'telemetry/event'")
  List<String> getBqStreamingDocTypes();

  void setBqStreamingDocTypes(List<String> value);

  @Description("A comma-separated list of docTypes for which we will not accumulate an"
      + " additional_properties field before publishing to BigQuery;"
      + " this is especially useful for telemetry/main where we expect to send the"
      + " same payload to multiple tables, each with only a subset of the overall schema;"
      + " each docType must be qualified with a namespace like 'telemetry/main'")
  List<String> getBqStrictSchemaDocTypes();

  void setBqStrictSchemaDocTypes(List<String> value);

  @Description("File format for --outputType=file|stdout; must be one of"
      + " json (each line contains payload[String] and attributeMap[String,String]) or"
      + " text (each line is payload)")
  @Default.Enum("json")
  OutputFileFormat getOutputFileFormat();

  void setOutputFileFormat(OutputFileFormat value);

  @Description("Row format for --outputType=bigquery; must be one of"
      + " raw (each row contains payload[Bytes] and attributes as top level fields) or"
      + " decoded (each row contains payload[Bytes] and attributes as nested metadata fields) or"
      + " payload (each row is extracted from payload); defaults to payload")
  @Default.Enum("payload")
  TableRowFormat getOutputTableRowFormat();

  void setOutputTableRowFormat(TableRowFormat value);

  @Description("Name of time partitioning field of error destination tables;"
      + " defaults to submission_timestamp")
  String getErrorBqPartitioningField();

  void setErrorBqPartitioningField(String value);

  @Description("Comma-separated list of clustering fields for error destination table;"
      + " defaults to submission_timestamp")
  List<String> getErrorBqClusteringFields();

  void setErrorBqClusteringFields(List<String> value);

  @Description("Method of writing to BigQuery for error output")
  @Default.Enum("file_loads")
  BigQueryWriteMethod getErrorBqWriteMethod();

  void setErrorBqWriteMethod(BigQueryWriteMethod value);

  @Description("How often to load a batch of files to BigQuery when writing errors via file_loads")
  @Default.String("5m")
  String getErrorBqTriggeringFrequency();

  void setErrorBqTriggeringFrequency(String value);

  @Description("Number of file shards to stage for BigQuery when writing errors via file_loads")
  @Default.Integer(100)
  int getErrorBqNumFileShards();

  void setErrorBqNumFileShards(int value);

  @Description("Compression format for --outputType=file")
  @Default.Enum("GZIP")
  Compression getOutputFileCompression();

  void setOutputFileCompression(Compression value);

  @Description("Number of output shards for --outputType=file; only relevant for stream"
      + " processing (--inputType=pubsub); in batch mode, the runner determines sharding")
  @Default.Integer(100)
  Integer getOutputNumShards();

  void setOutputNumShards(Integer value);

  @Description("Type of --errorOutput; must be one of [pubsub, file, bigquery]")
  @Default.Enum("pubsub")
  ErrorOutputType getErrorOutputType();

  void setErrorOutputType(ErrorOutputType value);

  @Description("Compression format for --errorOutputType=file")
  @Default.Enum("GZIP")
  Compression getErrorOutputFileCompression();

  void setErrorOutputFileCompression(Compression value);

  @Description("Number of output shards for --errorOutputType=file; only relevant for stream"
      + " processing (--inputType=pubsub); in batch mode, the runner determines sharding")
  @Default.Integer(100)
  Integer getErrorOutputNumShards();

  void setErrorOutputNumShards(Integer value);

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

  @Description("Attribute for built-in deduplication when reading from Pub/Sub."
      + " Must be an attribute set before the message was last published to Pub/Sub.")
  String getPubsubIdAttribute();

  void setPubsubIdAttribute(String value);

  @Description("Input to read from (path to file, PubSub subscription, etc.)")
  @Validation.Required
  String getInput();

  void setInput(String value);

  @Description("Output to write to (path to file or directory, Pubsub topic, etc.)")
  String getOutput();

  void setOutput(String value);

  @Description("Error output to write to (path to file or directory, Pubsub topic, etc.)")
  String getErrorOutput();

  void setErrorOutput(String value);

  @Description("Unless set to false, we will always attempt to decompress gzipped payloads")
  @Default.Boolean(true)
  Boolean getDecompressInputPayloads();

  void setDecompressInputPayloads(Boolean value);

  @Description("Compression format for payloads when --outputType=pubsub; defaults to GZIP")
  @Default.Enum("GZIP")
  Compression getOutputPubsubCompression();

  void setOutputPubsubCompression(Compression value);

  @Description("Compression format for payloads when --errorOutputType=pubsub; defaults to GZIP")
  @Default.Enum("GZIP")
  Compression getErrorOutputPubsubCompression();

  void setErrorOutputPubsubCompression(Compression value);

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

    @JsonIgnore
    Duration getParsedErrorBqTriggeringFrequency();

    void setParsedErrorBqTriggeringFrequency(Duration value);
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
    Optional.ofNullable(options.getWindowDuration()).map(Time::parseDuration)
        .ifPresent(options::setParsedWindowDuration);
    Optional.ofNullable(options.getBqTriggeringFrequency()).map(Time::parseDuration)
        .ifPresent(options::setParsedBqTriggeringFrequency);
    Optional.ofNullable(options.getErrorBqTriggeringFrequency()).map(Time::parseDuration)
        .ifPresent(options::setParsedErrorBqTriggeringFrequency);
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
}
