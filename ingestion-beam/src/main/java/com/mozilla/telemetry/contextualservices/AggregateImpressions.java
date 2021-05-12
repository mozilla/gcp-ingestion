package com.mozilla.telemetry.contextualservices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
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
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
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
      ReportingUrlUtil.PARAM_COUNTRY_CODE, ReportingUrlUtil.PARAM_REGION_CODE,
      ReportingUrlUtil.PARAM_FORM_FACTOR, ReportingUrlUtil.PARAM_OS_FAMILY,
      ReportingUrlUtil.PARAM_ID);

  private final ValueProvider<Integer> getAggregationWindowSize;

  public AggregateImpressions(ValueProvider<Integer> getAggregationWindowSize) {
    this.getAggregationWindowSize = getAggregationWindowSize;
  }

  public static AggregateImpressions of(ValueProvider<Integer> getAggregationWindowSize) {
    return new AggregateImpressions(getAggregationWindowSize);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> messages) {
    return messages.apply(ParDo.of(new AddAggregationKey())) //
        .apply(
            Window.into(FixedWindows.of(Duration.standardMinutes(getAggregationWindowSize.get())))) //
        .apply(GroupByKey.create()) //
        .apply(ParDo.of(new CountImpressionsPerKey())) //
        .apply(Window.into(new GlobalWindows()));
  }

  private class AddAggregationKey extends DoFn<PubsubMessage, KV<String, PubsubMessage>> {

    @ProcessElement
    public void processElement(@Element PubsubMessage message,
        OutputReceiver<KV<String, PubsubMessage>> out) {
      message = PubsubConstraints.ensureNonNull(message);

      String reportingUrl = message.getAttribute(Attribute.REPORTING_URL);

      ReportingUrlUtil urlParser = new ReportingUrlUtil(reportingUrl);

      // Rebuild url filtering out unneeded parameters
      ReportingUrlUtil aggregationUrl = new ReportingUrlUtil(urlParser.getBaseUrl());
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
      ReportingUrlUtil urlParser = new ReportingUrlUtil(input.getKey());

      int impressionCount = Iterables.size(input.getValue());
      long windowStart = window.start().getMillis();
      long windowEnd = window.end().getMillis();

      urlParser.addQueryParam(ReportingUrlUtil.PARAM_IMPRESSIONS,
          Integer.toString(impressionCount));
      urlParser.addQueryParam(ReportingUrlUtil.PARAM_TIMESTAMP_BEGIN,
          Long.toString(windowStart / 1000));
      urlParser.addQueryParam(ReportingUrlUtil.PARAM_TIMESTAMP_END,
          Long.toString(windowEnd / 1000));

      Map<String, String> attributeMap = ImmutableMap.of(Attribute.REPORTING_URL,
          urlParser.toString());

      ObjectNode json = Json.createObjectNode(); // TODO: for debug output
      json.put(Attribute.REPORTING_URL, urlParser.toString());
      json.put(Attribute.SUBMISSION_TIMESTAMP, Time.epochNanosToTimestamp(windowEnd / 1000));
      json.put(Attribute.DOCUMENT_ID, ":)");

      out.output(new PubsubMessage(Json.asBytes(json), attributeMap));
    }
  }
}
