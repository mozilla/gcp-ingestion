package com.mozilla.telemetry.util;

import static com.mozilla.telemetry.ingestion.core.util.Time.parseJavaDuration;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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

  /**
   * Attempts to parse a string in format '2011-12-03T10:15:30Z', returning null in case of error.
   */
  public static Instant parseAsInstantOrNull(String timestamp) {
    try {
      return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(timestamp));
    } catch (DateTimeParseException | NullPointerException ignore) {
      return null;
    }
  }

  /**
   * Returns a timestamp of form '2011-12-03T10:15:30Z' based on microseconds since Unix epoch.
   */
  public static String epochMicrosToTimestamp(Long epochMicros) {
    try {
      long epochSeconds = epochMicros / 1_000_000;
      long nanos = epochMicros % 1_000_000 * 1000;
      return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(epochSeconds, nanos));
    } catch (DateTimeParseException | NullPointerException ignore) {
      return null;
    }
  }

  /**
   * Returns a timestamp of form '2011-12-03T10:15:30Z' based on nanoseconds since Unix epoch.
   */
  public static String epochNanosToTimestamp(Long epochNanos) {
    try {
      long epochSeconds = epochNanos / 1_000_000_000;
      long nanos = epochNanos % 1_000_000_000;
      return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(epochSeconds, nanos));
    } catch (DateTimeParseException | NullPointerException ignore) {
      return null;
    }
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

}
