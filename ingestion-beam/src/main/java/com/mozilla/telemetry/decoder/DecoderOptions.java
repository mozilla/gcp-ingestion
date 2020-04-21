package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mozilla.telemetry.options.SinkOptions;
import java.net.URI;
import java.util.Optional;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;

/**
 * Options supported by {@code Decoder}.
 *
 * <p>Inherits standard configuration options and {@code Sink} configuration options.
 */
public interface DecoderOptions extends SinkOptions, PipelineOptions {

  @Description("Path (local or gs://) to GeoIP2-City.mmdb")
  @Validation.Required
  ValueProvider<String> getGeoCityDatabase();

  void setGeoCityDatabase(ValueProvider<String> value);

  @Description("Path (local or gs://) to newline-delimited text file listing city names to allow"
      + " in geoCity information; cities not in the list are considered too small to ensure"
      + " user anonymity so we won't report geoCity in that case."
      + " If not specified, no limiting is performed and we always report valid geoCity values.")
  ValueProvider<String> getGeoCityFilter();

  void setGeoCityFilter(ValueProvider<String> value);

  @Description("URI of a redis server that will be used for deduplication; leave null to disable")
  ValueProvider<String> getRedisUri();

  void setRedisUri(ValueProvider<String> value);

  @Description("If set to true, enable decryption of Pioneer payloads.")
  @Default.Boolean(false)
  Boolean getPioneerEnabled();

  void setPioneerEnabled(Boolean value);

  @Description("Path (local or gs://) to JSON array of metadata entries enumerating encrypted"
      + " private keys, Cloud KMS resource ids for decrypting those keys, and their corresponding"
      + " document namespaces; leave null to disable.")
  ValueProvider<String> getPioneerMetadataLocation();

  void setPioneerMetadataLocation(ValueProvider<String> value);

  @Description("If set to true, assume that all private keys are encrypted with the associated"
      + " KMS resourceId. Otherwise ignore KMS and assume all private keys are stored in plaintext."
      + " This may be used for debugging.")
  @Default.Boolean(true)
  ValueProvider<Boolean> getPioneerKmsEnabled();

  void setPioneerKmsEnabled(ValueProvider<Boolean> value);

  @Hidden
  @Description("Decompress pioneer pings.")
  @Default.Boolean(true)
  ValueProvider<Boolean> getPioneerDecompressPayload();

  void setPioneerDecompressPayload(ValueProvider<Boolean> value);

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

    @JsonIgnore
    ValueProvider<URI> getParsedRedisUri();

    void setParsedRedisUri(ValueProvider<URI> value);
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
    options.setParsedRedisUri(NestedValueProvider.of(options.getRedisUri(),
        s -> Optional.ofNullable(s).map(URI::create).orElse(null)));
  }

}
