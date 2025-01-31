package com.mozilla.telemetry.amplitude;

import com.mozilla.telemetry.options.SinkOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;

public interface AmplitudePublisherOptions extends SinkOptions, PipelineOptions {

  @Description("Path (local or gs://) to CSV text file containing events "
      + "that should be published to Amplitude")
  String getEventsAllowList();

  void setEventsAllowList(String value);

  @Description("Comma-separated strings representing a list of doc types for which "
      + " to send reporting requests; doc types are not namespace qualified "
      + "(e.g. quicksuggest-click is a correct argument)")
  String getAllowedDocTypes();

  void setAllowedDocTypes(String value);

  @Description("Comma-separated strings representing a list of namespaces for which "
      + " to send reporting requests (e.g. contextual-services is a correct argument)")
  @Default.String("contextual-services")
  String getAllowedNamespaces();

  void setAllowedNamespaces(String value);

  @Description("If set to true, send HTTP requests to Amplitude API.  "
      + "Can be set to false for testing purposes.")
  @Default.Boolean(true)
  Boolean getReportingEnabled();

  void setReportingEnabled(Boolean value);

  @Description("Duration window for batching reporting requests.")
  @Default.String("10m")
  String getBatchWindowDuration();

  void setBatchWindowDuration(String value);

  @Hidden
  interface Parsed extends AmplitudePublisherOptions, SinkOptions.Parsed {
  }

  /**
   * Return the input {@link AmplitudePublisherOptions} instance promoted to a
   * {@link AmplitudePublisherOptions.Parsed} and with all derived fields set.
   */
  static AmplitudePublisherOptions.Parsed parseAmplitudePublisherOptions(
      AmplitudePublisherOptions options) {
    final AmplitudePublisherOptions.Parsed parsed = options
        .as(AmplitudePublisherOptions.Parsed.class);
    SinkOptions.enrichSinkOptions(parsed);
    return parsed;
  }
}
