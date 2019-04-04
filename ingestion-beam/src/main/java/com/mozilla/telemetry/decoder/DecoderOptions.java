/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.primitives.Ints;
import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.util.Time;
import java.net.URI;
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

  @Description("Path (local or gs://) to a .tar.gz file containing json schemas; the expected"
      + " format is the output of GitHub's archive endpoint for mozilla-pipeline-schemas:"
      + " https://github.com/mozilla-services/mozilla-pipeline-schemas/archive/dev.tar.gz")
  ValueProvider<String> getSchemasLocation();

  void setSchemasLocation(ValueProvider<String> value);

  @Description("Path (local or gs://) to a .json file containing list of json schema aliases."
      + " Example file: schemaAliasing/example-aliasing-config.json"
      + " If not specified, no schemas will be aliased.")
  ValueProvider<String> getSchemaAliasesLocation();

  void setSchemaAliasesLocation(ValueProvider<String> value);

  @Description("Source of messages to mark as seen for deduplication. Allowed sources are:"
      + " pubsub (mark messages as seen from --deliveredMessagesSubscription),"
      + " immediate (mark messages as seen without waiting for delivery),"
      + " none (don't mark messages as seen, only remove messages already in redis).")
  @Default.Enum("pubsub")
  SeenMessagesSource getSeenMessagesSource();

  void setSeenMessagesSource(SeenMessagesSource value);

  @Description("PubSub subscription for a topic that contains messages delivered to --output")
  ValueProvider<String> getDeliveredMessagesSubscription();

  void setDeliveredMessagesSubscription(ValueProvider<String> value);

  @Description("URI of a redis server that will be used for deduplication")
  @Validation.Required
  ValueProvider<String> getRedisUri();

  void setRedisUri(ValueProvider<String> value);

  @Description("Duration for which message ids should be stored for deduplication."
      + " Allowed formats are: Ns (for seconds, example: 5s),"
      + " Nm (for minutes, example: 12m), Nh (for hours, example: 2h).")
  @Default.String("24h")
  ValueProvider<String> getDeduplicateExpireDuration();

  void setDeduplicateExpireDuration(ValueProvider<String> value);

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
    ValueProvider<Integer> getDeduplicateExpireSeconds();

    void setDeduplicateExpireSeconds(ValueProvider<Integer> value);

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
    options
        .setDeduplicateExpireSeconds(NestedValueProvider.of(options.getDeduplicateExpireDuration(),
            value -> Ints.checkedCast(Time.parseSeconds(value))));
    options.setParsedRedisUri(NestedValueProvider.of(options.getRedisUri(), URI::create));
  }

}
