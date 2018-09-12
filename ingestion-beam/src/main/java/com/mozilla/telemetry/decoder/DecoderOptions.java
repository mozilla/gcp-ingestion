package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.options.SinkOptions;
import java.net.URI;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface DecoderOptions extends SinkOptions {
  @Description("Path to GeoIP2-City.mmdb")
  @Validation.Required
  String getGeoCityDatabase();
  void setGeoCityDatabase(String value);

  @Description("Source of messages to mark as seen for deduplication. "
      + "Defaults to pubsub. "
      + "Allowed sources are: "
      + "pubsub (mark messages as seen from --deliveredMessagesSubscription), "
      + "immediate (mark messages as seen without waiting for delivery), "
      + "none (don't mark messages as seen, only remove messages already in redis).")
  @Default.Enum("pubsub")
  SeenMessagesSource getSeenMessagesSource();
  void setSeenMessagesSource(SeenMessagesSource value);

  @Description("PubSub subscription for a topic that contains messages delivered to --output")
  ValueProvider<String> getDeliveredMessagesSubscription();
  void setDeliveredMessagesSubscription(ValueProvider<String> value);

  @Description("URI of a redis server that will be used for deduplication")
  @Validation.Required
  ValueProvider<URI> getRedisUri();
  void setRedisUri(ValueProvider<URI> value);

  @Description("Duration for which message ids should be stored for deduplication. "
      + "Defaults to 24h. "
      + "Allowed formats are: "
      + "Ns (for seconds, example: 5s), "
      + "Nm (for minutes, example: 12m), "
      + "Nh (for hours, example: 2h).")
  @Default.String("24h")
  ValueProvider<String> getDeduplicateExpireDuration();
  void setDeduplicateExpireDuration(ValueProvider<String> value);
}
