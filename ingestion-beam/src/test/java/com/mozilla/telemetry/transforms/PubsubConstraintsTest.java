package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class PubsubConstraintsTest {

  private static final String ASCII_250 = StringUtils.repeat("abcdefghij", 25);
  private static final String ASCII_1000 = StringUtils.repeat(ASCII_250, 4);
  private static final String ASCII_1500 = StringUtils.repeat(ASCII_250, 6);

  // Euro is 3 bytes in UTF-8.
  private static final char EURO = '€';

  private void expectNonNull(PubsubMessage message, boolean expectSameMessage,
      String expectedPayload, Map<String, String> expectedAttributeMap, String expectedMessageId) {
    assertEquals(expectedPayload,
        new String(PubsubConstraints.ensureNonNull(message).getPayload(), StandardCharsets.UTF_8));
    assertEquals(expectedAttributeMap, PubsubConstraints.ensureNonNull(message).getAttributeMap());
    assertEquals(expectedMessageId, PubsubConstraints.ensureNonNull(message).getMessageId());
    if (expectSameMessage) {
      assertEquals(message, PubsubConstraints.ensureNonNull(message));
    } else {
      assertNotEquals(message, PubsubConstraints.ensureNonNull(message));
    }
  }

  @Test
  public void testEnsureNonNull() {
    final byte[] emptyBytes = new byte[] {};
    final byte[] testBytes = "test".getBytes(StandardCharsets.UTF_8);
    final Map<String, String> emptyMap = ImmutableMap.of();
    final Map<String, String> nonEmptyMap = ImmutableMap.of("k", "v");

    // PubsubMessage enforces non-null payload
    assertThrows(NullPointerException.class, () -> new PubsubMessage(null, emptyMap, "1"));

    expectNonNull(null, false, "", emptyMap, null);
    expectNonNull(new PubsubMessage(emptyBytes, null, null), false, "", emptyMap, null);
    expectNonNull(new PubsubMessage(testBytes, null, "1"), false, "test", emptyMap, "1");
    expectNonNull(new PubsubMessage(emptyBytes, emptyMap, null), true, "", emptyMap, null);
    expectNonNull(new PubsubMessage(testBytes, emptyMap, "2"), true, "test", emptyMap, "2");
    expectNonNull(new PubsubMessage(emptyBytes, nonEmptyMap, null), true, "", nonEmptyMap, null);
    expectNonNull(new PubsubMessage(testBytes, nonEmptyMap, "3"), true, "test", nonEmptyMap, "3");
  }

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
