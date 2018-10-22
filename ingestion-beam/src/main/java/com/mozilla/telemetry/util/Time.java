/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;

public class Time {

  /**
   * Parses a duration from a pseudo-ISO-8601 duration string.
   *
   * <p>Accepts any real ISO-8601 duration string as discussed in
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-">Duration.parse()</a>
   * but will also accept a variety of shorter formats:
   *
   * <p>5 seconds can be "5s", "5 seconds", "5 sec"<br>
   * 13 minutes can be "13m", "13 minutes", "13 min"<br>
   * 2 hours can be "2h", "2 hours", "2 hour"<br>
   * 500 milliseconds can be "0.5s"
   *
   * <p>Returns a Joda-time duration because that's what the Beam API expects, but internally uses
   * the Java time API so that we can easily switch to java time if a future version of Beam
   * removes Java 7 compatibility and accepts java time values.
   *
   * <p>Inspired by
   * <a href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/304bdbf4a3e2eac78adc4b06f466da82b656ef2c/src/main/java/com/google/cloud/teleport/util/DurationUtils.java">teleport DurationUtils</a>
   * and
   * <a href="https://github.com/dropwizard/dropwizard/blob/v1.3.5/dropwizard-util/src/main/java/io/dropwizard/util/Duration.java">dropwizard-util Duration</a>.
   *
   * @param value The duration value to parse.
   * @return The {@link org.joda.time.Duration} parsed from the supplied string.
   * @see java.time.Duration#parse(CharSequence)
   */
  public static org.joda.time.Duration parseDuration(String value) {
    return parseJodaDuration(value);
  }

  /**
   * Like {@link #parseDuration(String)}, but using a {@link ValueProvider}.
   *
   * <p>The value will be parsed once when first used; the result is cached and reused.
   */
  public static ValueProvider<org.joda.time.Duration> parseDuration(ValueProvider<String> value) {
    return NestedValueProvider.of(value, Time::parseDuration);
  }

  /**
   * Like {@link #parseDuration(String)}, but returns the number of seconds in the parsed duration.
   */
  public static long parseSeconds(String value) {
    return parseJavaDuration(value).getSeconds();
  }

  /**
   * Like {@link #parseSeconds(String)}, but using a {@link ValueProvider}.
   *
   * <p>The value will be parsed once when first used; the result is cached and reused.
   */
  public static ValueProvider<Long> parseSeconds(ValueProvider<String> value) {
    return NestedValueProvider.of(value, Time::parseSeconds);
  }

  /*
   * Private methods.
   */

  private static org.joda.time.Duration toJoda(java.time.Duration duration) {
    return org.joda.time.Duration.millis(duration.toMillis());
  }

  private static org.joda.time.Duration parseJodaDuration(String value) {
    return toJoda(parseJavaDuration(value));
  }

  private static java.time.Duration parseJavaDuration(String value) {
    checkNotNull(value, "The specified duration must be a non-null value!");
    java.time.Duration duration;

    try {
      // This is already an ISO-8601 duration.
      duration = java.time.Duration.parse(value);
    } catch (java.time.format.DateTimeParseException outer) {
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
        modifiedValue += "0S";
      }
      try {
        duration = java.time.Duration.parse(modifiedValue);
      } catch (java.time.format.DateTimeParseException e) {
        throw new IllegalArgumentException(
            "User-provided duration '" + value + "' was transformed to '" + modifiedValue
                + "', but java.time.Duration.parse() could not understand it.",
            e);
      }
    }

    checkArgument(duration.toMillis() > 0, "The window duration must be greater than 0!");

    return duration;
  }

}
