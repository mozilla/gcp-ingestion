/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class ParseSubmissionTimestampTest {

  @Test
  public void testEnrichedAttributes() {
    Map<String, String> attributes = ImmutableMap.of("submission_timestamp",
        "2018-03-12T21:02:18.123456Z");
    Map<String, String> enriched = ParseSubmissionTimestamp.enrichedAttributes(attributes);
    assertEquals("2018-03-12", enriched.get("submission_date"));
    assertEquals("21", enriched.get("submission_hour"));
  }

  @Test
  public void testNullMap() {
    Map<String, String> attributes = null;
    Map<String, String> enriched = ParseSubmissionTimestamp.enrichedAttributes(attributes);
    assertNull(enriched);
  }

  @Test
  public void testMissingAttribute() {
    Map<String, String> attributes = ImmutableMap.of();
    Map<String, String> enriched = ParseSubmissionTimestamp.enrichedAttributes(attributes);
    assertNull(enriched.get("submission_date"));
    assertNull(enriched.get("submission_hour"));
  }

  @Test
  public void testEmptyAttribute() {
    Map<String, String> attributes = ImmutableMap.of("submission_timestamp", "");
    Map<String, String> enriched = ParseSubmissionTimestamp.enrichedAttributes(attributes);
    assertNull(enriched.get("submission_date"));
    assertNull(enriched.get("submission_hour"));
  }

  @Test
  public void testDateOnly() {
    Map<String, String> attributes = ImmutableMap.of("submission_timestamp", "2018-03-12T");
    Map<String, String> enriched = ParseSubmissionTimestamp.enrichedAttributes(attributes);
    assertEquals("2018-03-12", enriched.get("submission_date"));
    assertNull(enriched.get("submission_hour"));
  }

}
