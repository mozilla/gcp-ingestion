package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.republisher.RepublisherOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Options supported by {@code Decoder}.
 *
 * <p>Inherits standard configuration options and {@code Republisher} configuration options.
 */
public interface DecoderOptions extends RepublisherOptions {

  @Description("Path to a gzipped tarball of schemas.")
  @Required
  String getSchemasLocation();

  void setSchemasLocation(String value);

  @Description("Path to a GeoIP2 City database.")
  @Required
  String getGeoCityDatabase();

  void setGeoCityDatabase(String value);

  @Description("Path to a GeoIP2 ISP database.")
  String getGeoIspDatabase();

  void setGeoIspDatabase(String value);

  @Description("Path to a file containing a whitelist of geoname IDs.")
  String getGeoCityFilter();

  void setGeoCityFilter(String value);

  @Description("If true, enable special handling for pings submitted via Cloud Logging.")
  @Default.Boolean(false)
  Boolean getLogIngestionEnabled();

  void setLogIngestionEnabled(Boolean value);

  /**
   * A custom {@link org.apache.beam.sdk.options.PipelineOptions} that includes derived fields.
   */
  @Hidden
  interface Parsed extends DecoderOptions, RepublisherOptions.Parsed {
  }

  /**
   * Return the input {@link DecoderOptions} instance promoted to a {@link
   * DecoderOptions.Parsed} and with all derived fields set.
   */
  static Parsed parseDecoderOptions(DecoderOptions options) {
    final Parsed parsed = options.as(Parsed.class);
    enrichDecoderOptions(parsed);
    return parsed;
  }

  /**
   * Set all the derived fields of a {@link DecoderOptions.Parsed} instance.
   */
  static void enrichDecoderOptions(Parsed options) {
    RepublisherOptions.enrichRepublisherOptions(options);
  }
}
