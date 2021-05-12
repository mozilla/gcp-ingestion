package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.options.SinkOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface ContextualServicesReporterOptions extends SinkOptions, PipelineOptions {

  @Description("Path (local or gs://) to CSV file text file containing allowed reporting "
      + "URLs to which requests can be sent to action type (click or impression),"
      + " expected format is url,type with no header")
  ValueProvider<String> getUrlAllowList();

  void setUrlAllowList(ValueProvider<String> value);

  @Description("Comma-separated strings representing a list of doc types for which "
      + " to send reporting requests; doc types are not namespace qualified "
      + "(e.g. quicksuggest-click is a correct argument)")
  ValueProvider<String> getAllowedDocTypes();

  void setAllowedDocTypes(ValueProvider<String> value);

  @Description("If set to true, send HTTP requests to reporting URLs.  "
      + "Can be set to false for testing purposes.")
  @Default.Boolean(true)
  ValueProvider<Boolean> getReportingEnabled();

  void setReportingEnabled(ValueProvider<Boolean> value);

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
