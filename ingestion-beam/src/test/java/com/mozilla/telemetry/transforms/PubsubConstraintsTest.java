/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertEquals;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class PubsubConstraintsTest {

  private static final String ASCII_250 = StringUtils.repeat("abcdefghij", 25);
  private static final String ASCII_1000 = StringUtils.repeat(ASCII_250, 4);

  // Euro is 3 bytes in UTF-8.
  private static final char EURO = '€';

  @Test
  public void truncateAttributeKey() {
    assertEquals("hi", PubsubConstraints.truncateAttributeKey("hi"));
    assertEquals(ASCII_250, PubsubConstraints.truncateAttributeKey(ASCII_250));
    assertEquals(ASCII_1000.substring(0, 253) + "...",
        PubsubConstraints.truncateAttributeKey(ASCII_1000));

    String unicode256Bytes = ASCII_250 + EURO + EURO;
    assertEquals(unicode256Bytes, PubsubConstraints.truncateAttributeKey(unicode256Bytes));

    String unicode259Bytes = ASCII_250 + EURO + EURO + EURO;
    String unicode259Truncated = ASCII_250 + EURO + "...";
    assertEquals(unicode259Truncated, PubsubConstraints.truncateAttributeKey(unicode259Bytes));
  }

  @Test
  public void truncateAttributeValue() {
    assertEquals("hi", PubsubConstraints.truncateAttributeValue("hi"));
    assertEquals(ASCII_250, ASCII_250);
    assertEquals(ASCII_1000, PubsubConstraints.truncateAttributeValue(ASCII_1000));

    String unicode256Bytes = ASCII_250 + EURO + EURO;
    assertEquals(unicode256Bytes, PubsubConstraints.truncateAttributeValue(unicode256Bytes));

    String unicode259Bytes = ASCII_250 + EURO + EURO + EURO;
    assertEquals(unicode259Bytes, PubsubConstraints.truncateAttributeValue(unicode259Bytes));

    String unicode1030Bytes = ASCII_1000 + StringUtils.repeat(EURO, 10);
    String unicode1030Truncated = ASCII_1000 + StringUtils.repeat(EURO, 7) + "...";
    assertEquals(unicode1030Truncated, PubsubConstraints.truncateAttributeValue(unicode1030Bytes));
  }
}
