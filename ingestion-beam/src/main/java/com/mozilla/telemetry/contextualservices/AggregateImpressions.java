package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Time;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Count the number of impressions aggregated by reporting URL.
 */
public class AggregateImpressions
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final List<String> aggregationFields = ImmutableList.of(
      ReportingUrlParser.PARAM_COUNTRY_CODE, ReportingUrlParser.PARAM_REGION_CODE,
      ReportingUrlParser.PARAM_FORM_FACTOR, ReportingUrlParser.PARAM_OS_FAMILY,
      ReportingUrlParser.PARAM_ID);

  private final ValueProvider<Integer> aggregationWindowDuration;

  public AggregateImpressions(ValueProvider<Integer> aggregationWindowDuration) {
    this.aggregationWindowDuration = aggregationWindowDuration;
  }

  public static AggregateImpressions of(ValueProvider<Integer> aggregationWindowDuration) {
    return new AggregateImpressions(aggregationWindowDuration);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> messages) {
    return messages
        .apply(
            Window.into(FixedWindows.of(Duration.standardMinutes(aggregationWindowDuration.get()))))
        .apply(ParDo.of(new AddAggregationKey())).apply(GroupByKey.create())
        .apply(ParDo.of(new CountImpressionsPerKey()));
  }

  private class AddAggregationKey extends DoFn<PubsubMessage, KV<String, PubsubMessage>> {

    @ProcessElement
    public void processElement(@Element PubsubMessage message,
        OutputReceiver<KV<String, PubsubMessage>> out) {
      message = PubsubConstraints.ensureNonNull(message);

      String reportingUrl = message.getAttribute(Attribute.REPORTING_URL);

      ReportingUrlParser urlParser = new ReportingUrlParser(reportingUrl);

      // Rebuild url filtering out unneeded parameters
      ReportingUrlParser aggregationUrl = new ReportingUrlParser(urlParser.getBaseUrl() + "?");
      for (String name : aggregationFields) {
        aggregationUrl.addQueryParam(name, urlParser.getQueryParam(name));
      }

      out.output(KV.of(aggregationUrl.getReportingUrl().toString(), message));
    }
  }

  private class CountImpressionsPerKey
      extends DoFn<KV<String, Iterable<PubsubMessage>>, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element KV<String, Iterable<PubsubMessage>> input,
        OutputReceiver<PubsubMessage> out, IntervalWindow window) {
      ReportingUrlParser urlParser = new ReportingUrlParser(input.getKey());

      int impressionCount = Iterables.size(input.getValue());
      long windowStart = window.start().getMillis();
      long windowEnd = window.end().getMillis();

      urlParser.addQueryParam(ReportingUrlParser.PARAM_IMPRESSIONS,
          Integer.toString(impressionCount));
      urlParser.addQueryParam(ReportingUrlParser.PARAM_TIMESTAMP_BEGIN, Long.toString(windowStart));
      urlParser.addQueryParam(ReportingUrlParser.PARAM_TIMESTAMP_END, Long.toString(windowEnd));

      Map<String, String> attributeMap = ImmutableMap.of(Attribute.REPORTING_URL,
          urlParser.toString(), Attribute.SUBMISSION_TIMESTAMP,
          Time.epochNanosToTimestamp(windowEnd / 1000));

      out.output(new PubsubMessage(new byte[] {}, attributeMap));
    }
  }
}
