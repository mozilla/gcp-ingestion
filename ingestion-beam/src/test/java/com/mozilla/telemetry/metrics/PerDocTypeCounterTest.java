package com.mozilla.telemetry.metrics;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class PerDocTypeCounterTest {

  @Test
  public void getKeyTest() {
    Map<String, String> attributes = ImmutableMap //
        .of("document_namespace", "telemetry", //
            "document_type", "core", //
            "document_version", "10");
    String key = PerDocTypeCounter.getCounterKey(attributes, "my-name");
    assertEquals("telemetry/core_v10/my_name", key);
  }

  @Test
  public void getKeyVersionlessTest() {
    Map<String, String> attributes = ImmutableMap //
        .of("document_namespace", "telemetry", //
            "document_type", "core");
    String key = PerDocTypeCounter.getCounterKey(attributes, "my-name");
    assertEquals("telemetry/core/my_name", key);
  }

  @Test
  public void getKeyNullTest() {
    String key = PerDocTypeCounter.getCounterKey(null, "my-name");
    assertEquals("unknown_namespace/unknown_doctype/my_name", key);
  }
}
