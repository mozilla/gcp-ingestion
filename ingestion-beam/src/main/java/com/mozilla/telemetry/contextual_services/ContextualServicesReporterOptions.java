package com.mozilla.telemetry.contextual_services;

import com.mozilla.telemetry.options.SinkOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

import java.util.List;

public interface ContextualServicesReporterOptions extends SinkOptions, PipelineOptions {

  @Description("Path (local or gs://) to newline-delimited text file containing allowed reporting "
      + "URLs to which requests can be sent")
  ValueProvider<String> getUrlAllowList();

  void setUrlAllowList(ValueProvider<String> value);

  @Description("List of doc types for which to send reporting requests")
  ValueProvider<List<String>> getAllowedDocTypes();

  void setAllowedDocTypes(ValueProvider<List<String>> value);

  @Hidden
  interface Parsed extends ContextualServicesReporterOptions, SinkOptions.Parsed {
  }

  /**
   * Return the input {@link ContextualServicesReporterOptions} instance promoted to a
   * {@link ContextualServicesReporterOptions.Parsed} and with all derived fields set.
   */
  static ContextualServicesReporterOptions.Parsed parseContextualServicesReporterOptions(
      ContextualServicesReporterOptions options) {
    final ContextualServicesReporterOptions.Parsed parsed = options.as(ContextualServicesReporterOptions.Parsed.class);
    SinkOptions.enrichSinkOptions(parsed);
    return parsed;
  }
}
