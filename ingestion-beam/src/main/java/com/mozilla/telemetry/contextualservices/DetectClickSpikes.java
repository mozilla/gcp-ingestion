package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.WithCurrentTimestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Transform that maintains state per key in order to label suspicious clicks.
 */
public class DetectClickSpikes extends
    PTransform<PCollection<KV<String, PubsubMessage>>, PCollection<KV<String, PubsubMessage>>> {

  private final Integer maxClicks;
  private final Long windowMillis;
  private final Counter status64Counter = Metrics.counter(DetectClickSpikes.class,
      "click_status_64");

  /**
   * Composite transform that wraps {@code DetectClickSpikes} with keying by {@code context_id}.
   */
  public static PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> perContextId(
      Integer maxClicks, Duration windowDuration) {
    return PTransform.compose("DetectClickSpikesPerContextId", input -> input //
        .apply(WithKeys.of((message) -> message.getAttribute(Attribute.CONTEXT_ID))) //
        .setCoder(KvCoder.of(StringUtf8Coder.of(), PubsubMessageWithAttributesCoder.of()))
        .apply(WithCurrentTimestamp.of()) //
        .apply(DetectClickSpikes.of(maxClicks, windowDuration)) //
        .apply(Values.create()));
  }

  public static DetectClickSpikes of(Integer maxClicks, Duration windowDuration) {
    return new DetectClickSpikes(maxClicks, windowDuration);
  }

  private DetectClickSpikes(Integer maxClicks, Duration windowDuration) {
    this.maxClicks = maxClicks;
    this.windowMillis = windowDuration.getMillis();
  }

  /** Accesses and updates state, adding the current timestamp and cleaning expired values. */
  private List<Long> updateTimestampState(ValueState<List<Long>> state, Long currentMillis) {
    List<Long> timestamps = Stream
        // We add the current element timestamp to the existing state.
        .concat(Stream.of(currentMillis),
            Optional.ofNullable(state.read()).orElse(Collections.emptyList()).stream())
        // We filter out any timestamps that have fallen outside the window.
        .filter(ts -> (currentMillis - ts) < windowMillis)
        // We order with largest timestamps first.
        .sorted(Comparator.reverseOrder())
        // We conserve memory by keeping only the most recent clicks if we're over the limit.
        .limit(maxClicks + 1).collect(Collectors.toList());
    state.write(timestamps);
    return timestamps;
  }

  /** Updates the passed attribute map, adding click-status to the reporting URL. */
  private static void addClickStatusToReportingUrlAttribute(Map<String, String> attributes) {
    String reportingUrl = attributes.get(Attribute.REPORTING_URL);
    ParsedReportingUrl urlParser = new ParsedReportingUrl(reportingUrl);
    // "64" indicates a spike in clicks per the Conducive API spec.
    urlParser.addQueryParam("click-status", "64");
    attributes.put(Attribute.REPORTING_URL, urlParser.toString());
  }

  private class Fn extends DoFn<KV<String, PubsubMessage>, KV<String, PubsubMessage>> {

    // See https://beam.apache.org/documentation/programming-guide/#state-and-timers
    @StateId("click-state")
    private final StateSpec<ValueState<List<Long>>> clickState = StateSpecs.value();
    @TimerId("click-timer")
    private final TimerSpec clickTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(@Element KV<String, PubsubMessage> element, @Timestamp Instant elementTs,
        @StateId("click-state") ValueState<List<Long>> state, @TimerId("click-timer") Timer timer,
        OutputReceiver<KV<String, PubsubMessage>> out) {
      List<Long> timestamps = updateTimestampState(state, elementTs.getMillis());

      // Set an event-time timer to clear state after windowMillis if no further clicks
      // are seen for this key. If another element with this key arrives,
      // it will overwrite this timer value.
      timer.set(Instant.ofEpochMilli(elementTs.getMillis() + windowMillis));

      if (timestamps.size() <= maxClicks) {
        out.output(element);
      } else {
        PubsubMessage message = element.getValue();
        Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
        addClickStatusToReportingUrlAttribute(attributes);
        status64Counter.inc();
        out.output(KV.of(element.getKey(), new PubsubMessage(message.getPayload(), attributes)));
      }
    }

    @OnTimer("click-timer")
    public void onTimer(@StateId("click-state") ValueState<List<Long>> state) {
      // If this method has been invoked, it means the key in question hasn't been accessed
      // for a full window duration and we can safely clear its state to free up memory.
      state.clear();
    }
  }

  @Override
  public PCollection<KV<String, PubsubMessage>> expand(
      PCollection<KV<String, PubsubMessage>> input) {
    return input.apply(ParDo.of(new Fn()));
  }
}
