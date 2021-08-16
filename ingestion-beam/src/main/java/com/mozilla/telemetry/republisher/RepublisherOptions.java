package com.mozilla.telemetry.republisher;

import com.mozilla.telemetry.options.SinkOptions;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options supported by {@code Republisher}.
 *
 * <p>Inherits standard configuration options and {@code Sink} configuration options.
 */
public interface RepublisherOptions extends SinkOptions, PipelineOptions {

  @Description("If set, messages with an x_debug_id attribute will be republished to"
      + " --debugDestination")
  @Default.Boolean(false)
  Boolean getEnableDebugDestination();

  void setEnableDebugDestination(Boolean value);

  @Description("The topic (assuming --outputType=pubsub) where debug messages are republished;"
      + " requires --enableDebugDestination is set")
  String getDebugDestination();

  void setDebugDestination(String value);

  @Description("An output topic name (assuming --outputType=pubsub) for a random sample of"
      + " incoming messages; messages are chosen based on sample_id if present or by a number"
      + " generated via ThreadLocalRandom; requires that --randomSampleRatio is set")
  String getRandomSampleDestination();

  void setRandomSampleDestination(String value);

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

  @Description("A JSON-formatted map of document namespaces to output topic names (assuming"
      + " --outputType=pubsub) for per-namespace sampling; the verbose map representation is used"
      + " here to support cases where the destination topics are heterogeneous and may live in"
      + " different projects")
  Map<String, String> getPerNamespaceDestinations();

  void setPerNamespaceDestinations(Map<String, String> value);

  @Description("A JSON-formatted map of topic names  to output to document types (assuming"
      + " --outputType=pubsub) for per-docType sampling")
  Map<String, List<String>> getPerDocTypeDestinations();

  void setPerDocTypeDestinations(Map<String, List<String>> value);

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
  }

}
