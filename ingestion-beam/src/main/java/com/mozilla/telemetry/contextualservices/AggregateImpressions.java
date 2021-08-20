package com.mozilla.telemetry.contextualservices;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.transforms.WithCurrentTimestamp;
import com.mozilla.telemetry.util.Time;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

/**
 * Count the number of impressions aggregated by reporting URL.
 */
public class AggregateImpressions
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final String aggregationWindowDuration;

  public AggregateImpressions(String aggregationWindowDuration) {
    this.aggregationWindowDuration = aggregationWindowDuration;
  }

  public static AggregateImpressions of(String getAggregationWindowSize) {
    return new AggregateImpressions(getAggregationWindowSize);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> messages) {
    return messages //
        // Add reporting url as key, pubsub message as value
        .apply(WithKeys.of((SerializableFunction<PubsubMessage, String>) //
        AggregateImpressions::getAggregationKey)) //
        .setCoder(KvCoder.of(StringUtf8Coder.of(), PubsubMessageWithAttributesCoder.of())) //
        // Set timestamp to current time
        .apply(WithCurrentTimestamp.of()) //
        // Group impressions into timed windows
        .apply("IntervalWindow",
            Window.into(FixedWindows.of(Time.parseDuration(aggregationWindowDuration)))) //
        .apply(Count.perKey()) //
        // Create aggregated url by adding impression count as query parameter
        .apply(ParDo.of(new BuildAggregateUrl())) //
        .apply("Return to GlobalWindow", Window.into(new GlobalWindows()));
  }

  @VisibleForTesting
  static String getAggregationKey(PubsubMessage message) {
    message = PubsubConstraints.ensureNonNull(message);

    String reportingUrl = message.getAttribute(Attribute.REPORTING_URL);

    ParsedReportingUrl urlParser = new ParsedReportingUrl(reportingUrl);

    // Rebuild url, sorting query params for consistency across urls
    List<Map.Entry<String, String>> keyValues = urlParser.getQueryParams().entrySet().stream()
        .sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
    ParsedReportingUrl aggregationUrl = new ParsedReportingUrl(urlParser.getBaseUrl());
    for (Map.Entry<String, String> kv : keyValues) {
      aggregationUrl.addQueryParam(kv.getKey(), kv.getValue());
    }

    return aggregationUrl.toString();
  }

  @VisibleForTesting
  static class BuildAggregateUrl extends DoFn<KV<String, Long>, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element KV<String, Long> input, OutputReceiver<PubsubMessage> out,
        IntervalWindow window) {
      ParsedReportingUrl urlParser = new ParsedReportingUrl(input.getKey());

      long impressionCount = input.getValue();
      long windowStart = window.start().getMillis();
      long windowEnd = window.end().getMillis();

      urlParser.addQueryParam(ParsedReportingUrl.PARAM_IMPRESSIONS, Long.toString(impressionCount));
      urlParser.addQueryParam(ParsedReportingUrl.PARAM_TIMESTAMP_BEGIN,
          Long.toString(windowStart / 1000));
      urlParser.addQueryParam(ParsedReportingUrl.PARAM_TIMESTAMP_END,
          Long.toString(windowEnd / 1000));

      Map<String, String> attributeMap = ImmutableMap.of(Attribute.REPORTING_URL,
          urlParser.toString(), Attribute.SUBMISSION_TIMESTAMP,
          Time.epochMicrosToTimestamp(new Instant().getMillis() * 1000));

      out.output(new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributeMap));
    }
  }
}
