package com.mozilla.telemetry.republisher;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RandomSamplerTest {

  @Test
  public void filterBySampleIdOrRandomNumber() {
    assertTrue(RandomSampler.filterBySampleIdOrRandomNumber(0L, 0.01));
    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber(1L, 0.01));
    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber(99L, 0.01));

    assertTrue(RandomSampler.filterBySampleIdOrRandomNumber(0L, 0.05));
    assertTrue(RandomSampler.filterBySampleIdOrRandomNumber(4L, 0.05));
    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber(5L, 0.05));
    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber(99L, 0.05));

    assertTrue(RandomSampler.filterBySampleIdOrRandomNumber(99L, 1.0));

    assertTrue(RandomSampler.filterBySampleIdOrRandomNumber("0", 0.01));
    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber("1", 0.01));
    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber("99", 0.01));

    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber(-3L, 0.00));
    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber(400L, 0.00));
    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber("foo", 0.00));
    assertFalse(RandomSampler.filterBySampleIdOrRandomNumber((String) null, 0.00));
  }
}
