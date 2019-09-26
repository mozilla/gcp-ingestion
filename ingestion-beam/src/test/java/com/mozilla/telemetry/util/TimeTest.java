/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;

public class TimeTest {

  @Test
  public void instantiateTimeForCodeCoverage() {
    new Time();
  }

  @Test
  public void testParseDuration() {
    assertEquals(Time.parseDuration("13s").getMillis(), SECONDS.toMillis(13));
    assertEquals(Time.parseDuration("4m").getMillis(), MINUTES.toMillis(4));
    assertEquals(Time.parseDuration("1h").getMillis(), HOURS.toMillis(1));
    assertEquals(Time.parseDuration("P2D").getMillis(), DAYS.toMillis(2));
  }

  @Test
  public void testExtendedParseDuration() {
    assertEquals(Time.parseDuration("5.5 seconds").getMillis(), 5500L);
    assertEquals(Time.parseDuration("5.5 SECONDS").getMillis(), 5500L);
    assertEquals(Time.parseDuration("1 hour").getMillis(), HOURS.toMillis(1));
    assertEquals(Time.parseDuration("2 days").getMillis(), DAYS.toMillis(2));
    assertEquals(Time.parseDuration("2 day 0.001 second").getMillis(), DAYS.toMillis(2) + 1L);
    assertEquals(Time.parseDuration("4m13s").getMillis(),
        MINUTES.toMillis(4) + SECONDS.toMillis(13));
    assertEquals(Time.parseDuration("P2days").getMillis(), DAYS.toMillis(2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseDurationIllegalArgument() {
    Time.parseDuration("foo");
  }

  @Test
  public void testParseDurationValueProvider() {
    final ValueProvider<String> provider = StaticValueProvider.of("13s");
    assertEquals(Time.parseDuration(provider).get().getMillis(), SECONDS.toMillis(13));
  }

  @Test
  public void testParseSeconds() {
    assertEquals(Time.parseSeconds("13s"), 13L);
    assertEquals(Time.parseSeconds("4m"), MINUTES.toSeconds(4));
  }

  @Test
  public void testParseSecondsValueProvider() {
    final ValueProvider<String> provider = StaticValueProvider.of("13s");
    assertEquals(Time.parseSeconds(provider).get().intValue(), 13);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroThrows() {
    Time.parseDuration("0m");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeThrows() {
    Time.parseDuration("-PT6H");
  }

  @Test
  public void testEpochMicrosToTimestamp() {
    assertEquals("2020-01-12T21:02:18.123456Z", Time.epochMicrosToTimestamp(1578862938123456L));
  }
}
