/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;

public class TimeTest {

  @Test
  public void testParseDuration() {
    assertEquals(Time.parseDuration("13s").getMillis(), 13000L);
    assertEquals(Time.parseDuration("4m").getMillis(), 4 * 60 * 1000L);
    assertEquals(Time.parseDuration("1h").getMillis(), 60 * 60 * 1000L);
    assertEquals(Time.parseDuration("P2D").getMillis(), 2 * 24 * 60 * 60 * 1000L);
  }

  @Test
  public void testExtendedParseDuration() {
    assertEquals(Time.parseDuration("5.5 seconds").getMillis(), 5500L);
    assertEquals(Time.parseDuration("5.5 SECONDS").getMillis(), 5500L);
    assertEquals(Time.parseDuration("4m13s").getMillis(), 4 * 60 * 1000L + 13000L);
    assertEquals(Time.parseDuration("1 hour").getMillis(), 60 * 60 * 1000L);
    assertEquals(Time.parseDuration("2 days").getMillis(), 2 * 24 * 60 * 60 * 1000L);
    assertEquals(
        Time.parseDuration("2 day 0.001 second").getMillis(), 2 * 24 * 60 * 60 * 1000L + 1L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseDurationIllegalArgument() {
    Time.parseDuration("foo");
  }

  @Test
  public void testParseDurationValueProvider() {
    final ValueProvider<String> provider = StaticValueProvider.of("13s");
    assertEquals(Time.parseDuration(provider).get().getMillis(), 13000L);
  }

  @Test
  public void testParseSeconds() {
    assertEquals(Time.parseSeconds("13s"), 13L);
    assertEquals(Time.parseSeconds("4m"), 4 * 60L);
  }

  @Test
  public void testParseSecondsValueProvider() {
    final ValueProvider<String> provider = StaticValueProvider.of("13s");
    assertEquals(Time.parseSeconds(provider).get().intValue(), 13);
  }
}
