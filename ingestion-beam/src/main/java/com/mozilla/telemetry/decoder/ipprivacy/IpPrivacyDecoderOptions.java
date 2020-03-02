package com.mozilla.telemetry.decoder.ipprivacy;

import com.mozilla.telemetry.decoder.DecoderOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface IpPrivacyDecoderOptions extends DecoderOptions, PipelineOptions {

  @Description("Path (local or gs://) to a file containing the bytes that will be used as"
      + "the hash key for the client IP")
  ValueProvider<String> getClientIpHashKey();

  void setClientIpHashKey(ValueProvider<String> value);

  @Description("Path (local or gs://) to a file containing the bytes that will be used as"
      + "the hash key for the client IP")
  ValueProvider<String> getClientIdHashKey();

  void setClientIdHashKey(ValueProvider<String> value);

  @Hidden
  interface Parsed extends IpPrivacyDecoderOptions, DecoderOptions.Parsed {
  }

  /**
   * Return the input {@link IpPrivacyDecoderOptions} instance promoted to a
   * {@link IpPrivacyDecoderOptions.Parsed} and with all derived fields set.
   */
  static Parsed parseIpPrivacyDecoderOptions(IpPrivacyDecoderOptions options) {
    final Parsed parsed = options.as(Parsed.class);
    DecoderOptions.enrichDecoderOptions(parsed);
    return parsed;
  }

}
