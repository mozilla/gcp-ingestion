/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.metrics;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.junit.Test;

public class PerDocTypeCounterTest {

  @Test
  public void getCounterTest() {
    Map<String, String> attributes = ImmutableMap //
        .of("document_namespace", "telemetry", //
            "document_type", "core", //
            "document_version", "10");
    Counter counter = PerDocTypeCounter.getOrCreateCounter(attributes, "my-name");
    assertEquals("telemetry/core_v10/my_name", counter.getName().getName());
  }

  @Test
  public void getCounterVersionlessTest() {
    Map<String, String> attributes = ImmutableMap //
        .of("document_namespace", "telemetry", //
            "document_type", "core");
    Counter counter = PerDocTypeCounter.getOrCreateCounter(attributes, "my-name");
    assertEquals("telemetry/core/my_name", counter.getName().getName());
  }

  @Test
  public void getCounterNullTest() {
    Counter counter = PerDocTypeCounter.getOrCreateCounter(null, "my-name");
    assertEquals("unknown_namespace/unknown_doctype/my_name", counter.getName().getName());
  }
}
