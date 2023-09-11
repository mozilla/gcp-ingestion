package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.options.SinkOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;

public interface ContextualServicesReporterOptions extends SinkOptions, PipelineOptions {

  @Description("Path (local or gs://) to CSV file text file containing allowed reporting "
      + "URLs to which requests can be sent to action type (click or impression),"
      + " expected format is url,type with no header")
  String getUrlAllowList();

  void setUrlAllowList(String value);

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

  @Description("If set to true, send HTTP requests to reporting URLs.  "
      + "Can be set to false for testing purposes.")
  @Default.Boolean(true)
  Boolean getReportingEnabled();

  void setReportingEnabled(Boolean value);

  @Description("Duration window for aggregation of reporting requests.")
  @Default.String("10m")
  String getAggregationWindowDuration();

  void setAggregationWindowDuration(String value);

  @Description("Duration window when counting clicks for labeling spikes.")
  @Default.String("3m")
  String getClickSpikeWindowDuration();

  void setClickSpikeWindowDuration(String value);

  @Description("Click count threshold when labeling click spikes.")
  @Default.Integer(10)
  Integer getClickSpikeThreshold();

  void setClickSpikeThreshold(Integer value);

  @Description("Duration window when counting impressions for labeling spikes.")
  @Default.String("3m")
  String getImpressionSpikeWindowDuration();

  void setImpressionSpikeWindowDuration(String value);

  @Description("Impression count threshold when labeling impression spikes.")
  // The daily average expected impressions per user is around 20.
  // So if we see more than that in 3 minutes, that's suspicious.
  @Default.Integer(20)
  Integer getImpressionSpikeThreshold();

  void setImpressionSpikeThreshold(Integer value);

  @Description("If set to true, send successfully requested reporting URLs to"
      + " error output.  SendRequests stage does not continue if true.")
  @Default.Boolean(true)
  Boolean getLogReportingUrls();

  void setLogReportingUrls(Boolean value);

  @Description("If set to true, reject messages from the namespace contextual_services for firefox"
      + " versions >= 116.  This is to prevent double reporting messages available from FoG.")
  @Default.Boolean(false)
  Boolean getLimitLegacyDesktopVersion();

  void setLimitLegacyDesktopVersion(Boolean value);

  @Hidden
  interface Parsed extends ContextualServicesReporterOptions, SinkOptions.Parsed {
  }

  /**
   * Return the input {@link ContextualServicesReporterOptions} instance promoted to a
   * {@link ContextualServicesReporterOptions.Parsed} and with all derived fields set.
   */
  static ContextualServicesReporterOptions.Parsed parseContextualServicesReporterOptions(
      ContextualServicesReporterOptions options) {
    final ContextualServicesReporterOptions.Parsed parsed = options
        .as(ContextualServicesReporterOptions.Parsed.class);
    SinkOptions.enrichSinkOptions(parsed);
    return parsed;
  }
}
