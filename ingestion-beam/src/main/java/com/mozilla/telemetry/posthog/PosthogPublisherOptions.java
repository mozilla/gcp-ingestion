package com.mozilla.telemetry.posthog;

import com.mozilla.telemetry.options.SinkOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;

public interface PosthogPublisherOptions extends SinkOptions, PipelineOptions {

  @Description("Path (local or gs://) to CSV text file containing events that should be published to Posthog")
  String getEventsAllowList();

  void setEventsAllowList(String value);

  @Description("Path (local or gs://) to a text file containing the Posthog API key")
  String getApiKeys();

  void setApiKeys(String value);

  @Description("Comma-separated strings representing a list of doc types for which to send reporting requests; "
      + "doc types are not namespace qualified (e.g. quicksuggest-click is a correct argument)")
  String getAllowedDocTypes();

  void setAllowedDocTypes(String value);

  @Description("Comma-separated strings representing a list of namespaces for which to send reporting requests "
      + "(e.g. contextual-services is a correct argument)")
  @Default.String("contextual-services")
  String getAllowedNamespaces();

  void setAllowedNamespaces(String value);

  @Description("If set to true, send HTTP requests to Posthog API. "
      + "Can be set to false for testing purposes.")
  @Default.Boolean(true)
  Boolean getReportingEnabled();

  void setReportingEnabled(Boolean value);

  @Description("Maximum number of event batches sent to Posthog API per second.")
  @Default.Integer(10)
  Integer getMaxBatchesPerSecond();

  void setMaxBatchesPerSecond(Integer value);

  @Description("Maximum number of Posthog events in a single batch.")
  @Default.Integer(10)
  Integer getMaxEventBatchSize();

  void setMaxEventBatchSize(Integer value);

  @Description("Maximum buffering duration when batching events in seconds")
  @Default.Integer(1)
  Integer getMaxBufferingDuration();

  void setMaxBufferingDuration(Integer value);

  @Description("A sampling ratio between 0.0 and 1.0; if not set, no random sample is produced")
  Double getRandomSampleRatio();

  void setRandomSampleRatio(Double value);

  @Hidden
  interface Parsed extends PosthogPublisherOptions, SinkOptions.Parsed {
  }

  /**
   * Parse pipeline options.
   */
  static PosthogPublisherOptions.Parsed parsePosthogPublisherOptions(
      PosthogPublisherOptions options) {
    final PosthogPublisherOptions.Parsed parsed = options.as(PosthogPublisherOptions.Parsed.class);
    SinkOptions.enrichSinkOptions(parsed);
    return parsed;
  }
}
