package com.mozilla.telemetry.metrics;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.metrics.Counter;
import org.junit.Test;

public class KeyedCounterTest {

  @Test
  public void getCounterTest() {
    Counter counter = KeyedCounter.getOrCreateCounter("counter_key");
    assertEquals("counter_key", counter.getName().getName());
  }
}
