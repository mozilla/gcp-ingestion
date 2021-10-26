package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;

public class FilterByChannel
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final String allowedChannels;

  private static transient Set<String> allowedChannelsSet;

  private static final Counter passed = Metrics.counter(FilterByChannel.class,
      "channel_filter_passed");
  private static final Counter rejected = Metrics.counter(FilterByChannel.class,
      "channel_filter_rejected");

  public FilterByChannel(String allowedChannels) {
    this.allowedChannels = allowedChannels;
  }

  public static FilterByChannel of(String allowedChannels) {
    return new FilterByChannel(allowedChannels);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(ParDo.of(new Fn()));
  }

  private class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage message, OutputReceiver<PubsubMessage> out) {
      message = PubsubConstraints.ensureNonNull(message);
      if (allowedChannelsSet == null) {
        if (allowedChannels == null) {
          throw new IllegalArgumentException("Required --allowedChannels argument not found");
        }
        allowedChannelsSet = Arrays.stream(allowedChannels.split(","))
            .filter(StringUtils::isNotBlank).collect(Collectors.toSet());
      }

      if (allowedChannelsSet.contains(message.getAttribute(Attribute.NORMALIZED_CHANNEL))) {
        out.output(message);

        PerDocTypeCounter.inc(message.getAttributeMap(), "channel_filter_passed");
      } else {
        PerDocTypeCounter.inc(message.getAttributeMap(), "channel_filter_rejected");
      }
    }
  }
}
