package com.mozilla.telemetry.ingestion.core.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class DerivedAttributesMapTest {

  @Test
  public void testGet() {
    Map<String, String> attributes = ImmutableMap.of("submission_timestamp",
        "2018-03-12T21:02:18.123456Z");
    Map<String, String> derived = DerivedAttributesMap.of(attributes);
    assertEquals("2018-03-12", derived.get("submission_date"));
    assertEquals("21", derived.get("submission_hour"));
    assertNull(derived.get("nonexistent_attribute"));
    assertThrows(IllegalArgumentException.class, () -> derived.get(1));
  }

  @Test
  public void testGetOrDefault() {
    Map<String, String> attributes = ImmutableMap.of("submission_timestamp",
        "2018-03-12T21:02:18.123456Z");
    Map<String, String> derived = DerivedAttributesMap.of(attributes);
    assertEquals("2018-03-12", derived.getOrDefault("submission_date", "default"));
    assertEquals("21", derived.getOrDefault("submission_hour", "default"));
    assertEquals("default", derived.getOrDefault("nonexistent_attribute", "default"));
  }

  @Test
  public void testContainsKey() {
    Map<String, String> attributes = ImmutableMap.of("submission_timestamp",
        "2018-03-12T21:02:18.123456Z");
    Map<String, String> derived = DerivedAttributesMap.of(attributes);
    assertTrue(derived.containsKey("submission_timestamp"));
    assertTrue(derived.containsKey("submission_date"));
    assertTrue(derived.containsKey("submission_hour"));
    assertFalse(derived.containsKey("sample_id"));
  }

  @Test
  public void testNullMap() {
    Map<String, String> attributes = null;
    Map<String, String> derived = DerivedAttributesMap.of(attributes);
    assertNull(derived);
  }

  @Test
  public void testMissingAttribute() {
    Map<String, String> attributes = ImmutableMap.of();
    Map<String, String> derived = DerivedAttributesMap.of(attributes);
    assertNull(derived.get("submission_date"));
    assertNull(derived.get("submission_hour"));
  }

  @Test
  public void testEmptyAttribute() {
    Map<String, String> attributes = ImmutableMap.of("submission_timestamp", "");
    Map<String, String> derived = DerivedAttributesMap.of(attributes);
    assertNull(derived.get("submission_date"));
    assertNull(derived.get("submission_hour"));
  }

  @Test
  public void testDateOnly() {
    Map<String, String> attributes = ImmutableMap.of("submission_timestamp", "2018-03-12T");
    Map<String, String> derived = DerivedAttributesMap.of(attributes);
    assertEquals("2018-03-12", derived.get("submission_date"));
    assertNull(derived.get("submission_hour"));
  }

}
