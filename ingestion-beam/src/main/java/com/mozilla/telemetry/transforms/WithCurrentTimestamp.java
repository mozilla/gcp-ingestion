package com.mozilla.telemetry.transforms;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

public class WithCurrentTimestamp<T> extends PTransform<PCollection<T>, PCollection<T>> {

  public static <T> WithCurrentTimestamp<T> of() {
    return new WithCurrentTimestamp<>();
  }

  private WithCurrentTimestamp() {
  }

  @VisibleForTesting
  static Instant adjustedTimestamp(Instant inputTimestamp) {
    Instant timestamp = new Instant();
    if (timestamp.isBefore(inputTimestamp)) {
      // A DoFn will throw an error if it ever tries to shift an element backwards in time,
      // which could put it behind the watermark and make it considered "late", so we maintain
      // the inputTimestamp in cases where it appears to be ahead of the current server time.
      // See
      // https://beam.apache.org/releases/javadoc/2.29.0/org/apache/beam/sdk/transforms/WithTimestamps.html#getAllowedTimestampSkew--
      return inputTimestamp;
    } else {
      return timestamp;
    }
  }

  private static class Fn<T> extends DoFn<T, T> {

    @ProcessElement
    public void processElement(@Element T message, @Timestamp Instant inputTimestamp,
        OutputReceiver<T> out) {
      out.outputWithTimestamp(message, adjustedTimestamp(inputTimestamp));
    }
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input.apply(ParDo.of(new Fn<>()));
  }
}
