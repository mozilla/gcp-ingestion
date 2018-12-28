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
  private static final String ASCII_1500 = StringUtils.repeat(ASCII_250, 6);

  // Euro is 3 bytes in UTF-8.
  private static final char EURO = '€';

  @Test
  public void testTruncatePreservesShortAscii() {
    assertEquals("hi", PubsubConstraints.truncateAttributeKey("hi"));
    assertEquals("hi", PubsubConstraints.truncateAttributeValue("hi"));
    assertEquals(ASCII_250, PubsubConstraints.truncateAttributeKey(ASCII_250));
    assertEquals(ASCII_1000, PubsubConstraints.truncateAttributeValue(ASCII_1000));
  }

  @Test
  public void testTruncateAscii() {
    assertEquals(ASCII_1500.substring(0, 253) + "...",
        PubsubConstraints.truncateAttributeKey(ASCII_1500));
    assertEquals(ASCII_1500.substring(0, 1021) + "...",
        PubsubConstraints.truncateAttributeValue(ASCII_1500));
  }

  @Test
  public void testTruncatePreservesShortUnicode() {
    String unicode256Bytes = ASCII_250 + EURO + EURO;
    String unicode259Bytes = ASCII_250 + EURO + EURO + EURO;
    assertEquals(unicode256Bytes, PubsubConstraints.truncateAttributeKey(unicode256Bytes));
    assertEquals(unicode259Bytes, PubsubConstraints.truncateAttributeValue(unicode259Bytes));
  }

  @Test
  public void testTruncateUnicodeAligned() {
    // This string is 10 chars, but 30 bytes, so has 20 more bytes than chars.
    String euros = StringUtils.repeat(EURO, 10);

    String unicode1030Bytes = euros + StringUtils.repeat(ASCII_250, 4);
    assertEquals(unicode1030Bytes.substring(0, 256 - 20 - 3) + "...",
        PubsubConstraints.truncateAttributeKey(unicode1030Bytes));
    assertEquals(unicode1030Bytes.substring(0, 1024 - 20 - 3) + "...",
        PubsubConstraints.truncateAttributeValue(unicode1030Bytes));
  }

  @Test
  public void testTruncateUnicodeUnaligned() {
    String unicode257Bytes = ASCII_250 + "," + EURO + EURO;
    assertEquals(ASCII_250 + ",...", PubsubConstraints.truncateAttributeKey(unicode257Bytes));
  }
}
