package com.mozilla.telemetry.ingestion.core.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;
import java.time.format.DateTimeParseException;

public class Time {

  private Time() {
  }

  /**
   * Return a {@code java.time.Duration} from an ISO-8601 duration or from simple formats
   * such as "5 seconds", "5sec", or "5s".
   */
  public static Duration parseJavaDuration(String value) {
    checkNotNull(value, "The specified duration must be a non-null value!");
    Duration duration;

    try {
      // This is already an ISO-8601 duration.
      duration = Duration.parse(value);
    } catch (DateTimeParseException outer) {
      String modifiedValue = value.toLowerCase().replaceAll("seconds", "s")
          .replaceAll("second", "s").replaceAll("sec", "s").replaceAll("minutes", "m")
          .replaceAll("minute", "m").replaceAll("mins", "m").replaceAll("min", "m")
          .replaceAll("hours", "h").replaceAll("hour", "h").replaceAll("days", "dt")
          .replaceAll("day", "dt").replaceAll("\\s+", "").toUpperCase();
      if (!modifiedValue.contains("T")) {
        modifiedValue = "T" + modifiedValue;
      }
      if (!modifiedValue.contains("P")) {
        modifiedValue = "P" + modifiedValue;
      }
      if (modifiedValue.endsWith("T")) {
        modifiedValue += "0s";
      }
      try {
        duration = Duration.parse(modifiedValue);
      } catch (java.time.format.DateTimeParseException e) {
        throw new IllegalArgumentException(
            "User-provided duration '" + value + "' was transformed to '" + modifiedValue
                + "', but java.time.Duration.parse() could not understand it.",
            e);
      }
    }

    return duration;
  }

  /**
   * Perform {@code Time::parseJavaDuration} and require the result to be greater than 0.
   */
  public static Duration parsePositiveJavaDuration(String value) {
    Duration duration = parseJavaDuration(value);
    checkArgument(duration.toMillis() > 0, "The window duration must be greater than 0!");
    return duration;
  }
}
