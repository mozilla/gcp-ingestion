package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.transforms.WithCurrentTimestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
@SuppressWarnings("checkstyle:lineLength")
public class LabelSpikes extends
    PTransform<PCollection<KV<String, SponsoredInteraction>>, PCollection<KV<String, SponsoredInteraction>>> {

  private final Integer maxInteractions;
  private final Long windowMillis;
  private final Counter ghostClickCounter = Metrics.counter(LabelSpikes.class, "ghost_click");
  private String paramName;
  private String suspiciousParamValue;

  /**
   * Composite transform that wraps {@code DetectClickSpikes} with keying by {@code context_id}.
   */
  public static PTransform<PCollection<SponsoredInteraction>, PCollection<SponsoredInteraction>> perContextId(
      Integer maxClicks, Duration windowDuration, TelemetryEventType eventType) {
    return PTransform.compose("DetectClickSpikesPerContextId", input -> {
      try {
        return input //
            .apply(WithKeys.of((interaction) -> interaction.getContextId())) //
            .setCoder(KvCoder.of(StringUtf8Coder.of(), SponsoredInteraction.getCoder())) //
            .apply(WithCurrentTimestamp.of()) //
            .apply(LabelSpikes.of(maxClicks, windowDuration, eventType)) //
            .apply(Values.create());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static LabelSpikes of(Integer maxClicks, Duration windowDuration,
      TelemetryEventType eventType) throws Exception {
    return new LabelSpikes(maxClicks, windowDuration, eventType);
  }

  private LabelSpikes(Integer maxInteractions, Duration windowDuration,
      TelemetryEventType eventType) throws Exception {
    this.maxInteractions = maxInteractions;
    this.windowMillis = windowDuration.getMillis();
    switch (eventType) {
      case CLICK:
        this.paramName = BuildReportingUrl.PARAM_CLICK_STATUS;
        this.suspiciousParamValue = ParseReportingUrl.CLICK_STATUS_GHOST;
        break;
      case IMPRESSION:
        this.paramName = BuildReportingUrl.PARAM_IMPPRESSION_STATUS;
        this.suspiciousParamValue = ParseReportingUrl.IMPRESSION_STATUS_SUSPICIOUS;
        break;
      default:
        throw new Exception("The LabelSpikes class is only set up to evaluate click and impression eventTypes.");
    }
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
        .limit(maxInteractions + 1).collect(Collectors.toList());
    state.write(timestamps);
    return timestamps;
  }

  /** Updates the passed attribute map, adding a query param to the reporting URL. */
  private static String addStatusToReportingUrlAttribute(String reportingUrl, String paramName,
      String status) {
    BuildReportingUrl urlBuilder = new BuildReportingUrl(reportingUrl);
    urlBuilder.addQueryParam(paramName, status);
    return urlBuilder.toString();
  }

  private class Fn
      extends DoFn<KV<String, SponsoredInteraction>, KV<String, SponsoredInteraction>> {

    // See https://beam.apache.org/documentation/programming-guide/#state-and-timers
    @StateId("click-state")
    private final StateSpec<ValueState<List<Long>>> clickState = StateSpecs.value();
    @TimerId("click-timer")
    private final TimerSpec clickTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void process(@Element KV<String, SponsoredInteraction> element, //
        @Timestamp Instant elementTs, //
        @StateId("click-state") ValueState<List<Long>> state, //
        @TimerId("click-timer") Timer timer, //
        OutputReceiver<KV<String, SponsoredInteraction>> out) {
      List<Long> timestamps = updateTimestampState(state, elementTs.getMillis());

      // Set a processing-time timer to clear state after windowMillis if no further clicks
      // are seen for this key. If another element with this key arrives,
      // it will overwrite this timer value.
      timer.offset(Duration.millis(windowMillis)).setRelative();

      if (timestamps.size() <= maxInteractions) {
        out.output(element);
      } else {
        SponsoredInteraction interaction = element.getValue();

        String reportingUrl = addStatusToReportingUrlAttribute(interaction.getReportingUrl(),
            paramName, suspiciousParamValue);
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
