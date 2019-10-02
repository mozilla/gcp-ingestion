package com.mozilla.telemetry.republisher;

import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;

public class RandomSampler {

  /**
   * If the given sampleIdString is convertible to an integer between 0 and 99,
   * return whether the integer is less than {@code int(100 * ratio)}.
   * Otherwise, generate a random number between 0 and 1 and return whether the generated number
   * is less then the passed ratio.
   */
  public static boolean filterBySampleIdOrRandomNumber(@Nullable String sampleIdString,
      Double ratio) {
    Long sampleId = null;
    if (sampleIdString != null) {
      try {
        sampleId = Long.valueOf(sampleIdString);
      } catch (NumberFormatException ignore) {
        // pass
      }
    }
    return filterBySampleIdOrRandomNumber(sampleId, ratio);
  }

  /**
   * If the given sampleId is between 0 and 99,
   * return whether the integer is less than {@code int(100 * ratio)}.
   * Otherwise, generate a random number between 0 and 1 and return whether the generated number
   * is less then the passed ratio.
   */
  public static boolean filterBySampleIdOrRandomNumber(@Nullable Long sampleId, Double ratio) {
    if (sampleId != null && sampleId >= 0 && sampleId < 100) {
      long maxPercentile = Math.round(ratio * 100);
      return sampleId < maxPercentile;
    } else {
      return ThreadLocalRandom.current().nextDouble() < ratio;
    }
  }

}
