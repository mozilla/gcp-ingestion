package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.options.SinkOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by {@code Decoder}.
 *
 * <p>Inherits standard configuration options and {@code Sink} configuration options.
 */
public interface DecoderOptions extends SinkOptions, PipelineOptions {

  @Description("Comma-separated list of static IPs associated with load balancers; "
      + "will be ignored for IP lookups")
  @Default.String("35.227.207.240,34.120.208.123")
  String getProxyIps();

  void setProxyIps(String value);

  @Description("Path (local or gs://) to GeoIP2-City.mmdb")
  @Validation.Required
  String getGeoCityDatabase();

  void setGeoCityDatabase(String value);

  @Description("Path (local or gs://) to newline-delimited text file listing city names to allow"
      + " in geoCity information; cities not in the list are considered too small to ensure"
      + " user anonymity so we won't report geoCity in that case."
      + " If not specified, no limiting is performed and we always report valid geoCity values.")
  String getGeoCityFilter();

  void setGeoCityFilter(String value);

  @Description("Path (local or gs://) to GeoIP2-ISP.mmdb")
  @Validation.Required
  String getGeoIspDatabase();

  void setGeoIspDatabase(String value);

  @Description("If set to true, enable decryption of Pioneer payloads.")
  @Default.Boolean(false)
  Boolean getPioneerEnabled();

  void setPioneerEnabled(Boolean value);

  @Description("Path (local or gs://) to JSON array of metadata entries enumerating encrypted"
      + " private keys, Cloud KMS resource ids for decrypting those keys, and their corresponding"
      + " document namespaces; this must be set if Pioneer is enabled.")
  String getPioneerMetadataLocation();

  void setPioneerMetadataLocation(String value);

  @Description("If set to true, assume that all private keys are encrypted with the associated"
      + " KMS resourceId. Otherwise ignore KMS and assume all private keys are stored in plaintext."
      + " This may be used for debugging.")
  @Default.Boolean(true)
  Boolean getPioneerKmsEnabled();

  void setPioneerKmsEnabled(Boolean value);

  @Hidden
  @Description("Decompress pioneer pings.")
  @Default.Boolean(true)
  Boolean getPioneerDecompressPayload();

  void setPioneerDecompressPayload(Boolean value);

  @Description("If set to true, enable decryption of Account Ecosystem Telemetry identifiers.")
  @Default.Boolean(false)
  Boolean getAetEnabled();

  void setAetEnabled(Boolean value);

  @Description("Path (local or gs://) to JSON array of metadata entries enumerating encrypted"
      + " private keys, Cloud KMS resource ids for decrypting those keys, and their corresponding"
      + " document namespaces; this must be set if AET is enabled.")
  String getAetMetadataLocation();

  void setAetMetadataLocation(String value);

  @Description("If set to true, assume that all private keys are encrypted with the associated"
      + " KMS resourceId. Otherwise ignore KMS and assume all private keys are stored in plaintext."
      + " This may be used for debugging.")
  @Default.Boolean(true)
  Boolean getAetKmsEnabled();

  void setAetKmsEnabled(Boolean value);

  /*
   * Subinterface and static methods.
   */

  /**
   * A custom {@link PipelineOptions} that includes derived fields.
   *
   * <p>This class should only be instantiated from an existing {@link DecoderOptions} instance
   * via the static {@link #parseDecoderOptions(DecoderOptions)} method.
   * This follows a similar pattern to the Beam Spark runner's {@code SparkContextOptions}
   * which is instantiated from {@code SparkPipelineOptions} and then enriched.
   */
  @Hidden
  interface Parsed extends DecoderOptions, SinkOptions.Parsed {
  }

  /**
   * Return the input {@link DecoderOptions} instance promoted to a {@link DecoderOptions.Parsed}
   * and with all derived fields set.
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
    SinkOptions.enrichSinkOptions(options);
  }

}
