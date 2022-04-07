package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.transforms.WithCurrentTimestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
public class LabelClickSpikes extends
    PTransform<PCollection<KV<String, SponsoredInteraction>>,
      PCollection<KV<String, SponsoredInteraction>>> {

  private final Integer maxClicks;
  private final Long windowMillis;
  private final Counter ghostClickCounter = Metrics.counter(LabelClickSpikes.class, "ghost_click");

  /**
   * Composite transform that wraps {@code DetectClickSpikes} with keying by {@code context_id}.
   */
  public static PTransform<PCollection<SponsoredInteraction>,
          PCollection<SponsoredInteraction>> perContextId(
            Integer maxClicks,
            Duration windowDuration) {
    return PTransform.compose("DetectClickSpikesPerContextId", input -> input //
        .apply(WithKeys.of((interaction) -> interaction.getContextId())) //
        .apply(WithCurrentTimestamp.of()) //
        .apply(LabelClickSpikes.of(maxClicks, windowDuration)) //
        .apply(Values.create()));
  }

  public static LabelClickSpikes of(Integer maxClicks, Duration windowDuration) {
    return new LabelClickSpikes(maxClicks, windowDuration);
  }

  private LabelClickSpikes(Integer maxClicks, Duration windowDuration) {
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
        // We conserve memory by keeping only the most recent clicks if we're over the limit;
        // we allow (maxClicks + 1) elements so size checks can use ">" rather than ">=".
        .limit(maxClicks + 1).collect(Collectors.toList());
    state.write(timestamps);
    return timestamps;
  }

  /** Updates the passed attribute map, adding click-status to the reporting URL. */
  private static String addClickStatusToReportingUrlAttribute(String reportingUrl) {
    ParsedReportingUrl urlParser = new ParsedReportingUrl(reportingUrl);
    urlParser.addQueryParam(ParsedReportingUrl.PARAM_CLICK_STATUS,
        ParseReportingUrl.CLICK_STATUS_GHOST);
    return urlParser.toString();
  }

  private class Fn
      extends DoFn<KV<String, SponsoredInteraction>, KV<String, SponsoredInteraction>> {

    // See https://beam.apache.org/documentation/programming-guide/#state-and-timers
    @StateId("click-state")
    private final StateSpec<ValueState<List<Long>>> clickState = StateSpecs.value();
    @TimerId("click-timer")
    private final TimerSpec clickTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void process(@Element KV<String, SponsoredInteraction> element,
        @Timestamp Instant elementTs, @StateId("click-state") ValueState<List<Long>> state,
        @TimerId("click-timer") Timer timer, OutputReceiver<KV<String, SponsoredInteraction>> out) {
      List<Long> timestamps = updateTimestampState(state, elementTs.getMillis());

      // Set a processing-time timer to clear state after windowMillis if no further clicks
      // are seen for this key. If another element with this key arrives,
      // it will overwrite this timer value.
      timer.offset(Duration.millis(windowMillis)).setRelative();

      if (timestamps.size() <= maxClicks) {
        out.output(element);
      } else {
        SponsoredInteraction interaction = element.getValue();
        String reportingUrl = addClickStatusToReportingUrlAttribute(interaction.getReportingUrl());
        ghostClickCounter.inc();
        out.output(
            KV.of(element.getKey(), interaction.toBuilder().setReportingUrl(reportingUrl).build()));
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
  public PCollection<KV<String, SponsoredInteraction>> expand(
      PCollection<KV<String, SponsoredInteraction>> input) {
    return input.apply(ParDo.of(new Fn()));
  }
}
