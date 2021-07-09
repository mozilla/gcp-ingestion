package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertEquals;

import org.joda.time.Instant;
import org.junit.Test;

public class WithCurrentTimestampTest {

  @Test
  public void testAdjustedTimestamp() {
    Instant startTimestamp = new Instant();
    assertEquals(1.0 * startTimestamp.getMillis(),
        1.0 * WithCurrentTimestamp.adjustedTimestamp(new Instant(0)).getMillis(), 100.0);

    Instant maxInstant = new Instant(Long.MAX_VALUE);
    assertEquals(maxInstant.getMillis(),
        WithCurrentTimestamp.adjustedTimestamp(maxInstant).getMillis());
  }

}
