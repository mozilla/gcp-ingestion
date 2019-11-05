package com.mozilla.telemetry.republisher;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.primitives.Ints;
import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.util.Time;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;

/**
 * Options supported by {@code Republisher}.
 *
 * <p>Inherits standard configuration options and {@code Sink} configuration options.
 */
public interface RepublisherOptions extends SinkOptions, PipelineOptions {

  @Description("URI of a redis server that will be contacted to mark document IDs as seen for"
      + " deduplication purposes; if left unspecified, this step of the pipeline is skipped")
  @Validation.Required
  ValueProvider<String> getRedisUri();

  void setRedisUri(ValueProvider<String> value);

  @Description("Duration for which document IDs should be stored for deduplication."
      + " Allowed formats are: Ns (for seconds, example: 5s),"
      + " Nm (for minutes, example: 12m), Nh (for hours, example: 2h)."
      + " Can be omitted if --redisUri is unset.")
  @Default.String("24h")
  ValueProvider<String> getDeduplicateExpireDuration();

  void setDeduplicateExpireDuration(ValueProvider<String> value);

  @Description("If set, messages with an x_debug_id attribute will be republished to"
      + " --debugDestination")
  @Default.Boolean(false)
  Boolean getEnableDebugDestination();

  void setEnableDebugDestination(Boolean value);

  @Description("The topic (assuming --outputType=pubsub) where debug messages are republished;"
      + " requires --enableDebugDestination is set")
  ValueProvider<String> getDebugDestination();

  void setDebugDestination(ValueProvider<String> value);

  @Description("An output topic name (assuming --outputType=pubsub) for a random sample of"
      + " incoming messages; messages are chosen based on sample_id if present or by a number"
      + " generated via ThreadLocalRandom; requires that --randomSampleRatio is set")
  ValueProvider<String> getRandomSampleDestination();

  void setRandomSampleDestination(ValueProvider<String> value);

  @Description("A sampling ratio between 0.0 and 1.0; if not set, no random sample is produced")
  Double getRandomSampleRatio();

  void setRandomSampleRatio(Double value);

  @Description("A JSON-formatted map of channel name to sampling ratio; for example,"
      + " {\"nightly\":1.0,\"release\":0.01} would republish 100% of nightly pings to the"
      + " sampled nightly topic and 1% of release pings to the sampled release topic")
  Map<String, Double> getPerChannelSampleRatios();

  void setPerChannelSampleRatios(Map<String, Double> value);

  @Description("A pattern for output topic names (assuming --outputType=pubsub) for per-channel"
      + " sampling; the pattern must contain a placeholder ${channel} that will be filled in"
      + " to give a distinct publisher per channel configured in --perChannelSampleRatios")
  String getPerChannelDestination();

  void setPerChannelDestination(String value);

  @Description("A comma-separated list of docTypes that should be republished to individual"
      + " topics; each docType must be qualified with a namespace like 'telemetry/event'")
  List<String> getPerDocTypeEnabledList();

  void setPerDocTypeEnabledList(List<String> value);

  @Description("A pattern for output topic names (assuming --outputType=pubsub) for per-docType"
      + " sampling; the pattern must contain a placeholder ${document_type} and may optionally"
      + " contain a placeholder ${document_namespace} that will be filled in"
      + " to give a distinct publisher per docType configured in --perDocTypeEnabledList")
  String getPerDocTypeDestination();

  void setPerDocTypeDestination(String value);

  @Description("A JSON-formatted map of document namespaces to output topic names (assuming"
      + " --outputType=pubsub) for per-namespace sampling; the verbose map representation is used"
      + " here to support cases where the destination topics are heterogeneous and may live in"
      + " different projects")
  Map<String, String> getPerNamespaceDestinations();

  void setPerNamespaceDestinations(Map<String, String> value);

  /*
   * Subinterface and static methods.
   */

  /**
   * A custom {@link PipelineOptions} that includes derived fields.
   *
   * <p>This class should only be instantiated from an existing {@link RepublisherOptions} instance
   * via the static {@link #parseRepublisherOptions(RepublisherOptions)} method.
   * This follows a similar pattern to the Beam Spark runner's {@code SparkContextOptions}
   * which is instantiated from {@code SparkPipelineOptions} and then enriched.
   */
  @Hidden
  interface Parsed extends RepublisherOptions, SinkOptions.Parsed {

    @JsonIgnore
    ValueProvider<Integer> getDeduplicateExpireSeconds();

    void setDeduplicateExpireSeconds(ValueProvider<Integer> value);

    @JsonIgnore
    ValueProvider<URI> getParsedRedisUri();

    void setParsedRedisUri(ValueProvider<URI> value);
  }

  /**
   * Return the input {@link RepublisherOptions} instance promoted to a {@link
   * RepublisherOptions.Parsed} and with all derived fields set.
   */
  static Parsed parseRepublisherOptions(RepublisherOptions options) {
    final Parsed parsed = options.as(Parsed.class);
    enrichRepublisherOptions(parsed);
    return parsed;
  }

  /**
   * Set all the derived fields of a {@link RepublisherOptions.Parsed} instance.
   */
  static void enrichRepublisherOptions(Parsed options) {
    SinkOptions.enrichSinkOptions(options);
    options
        .setDeduplicateExpireSeconds(NestedValueProvider.of(options.getDeduplicateExpireDuration(),
            value -> Ints.checkedCast(Time.parseSeconds(value))));
    options.setParsedRedisUri(NestedValueProvider.of(options.getRedisUri(),
        s -> Optional.ofNullable(s).map(URI::create).orElse(null)));
  }

}
