/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.schemas;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;

public class SchemaStoreTest {

  private static final ValueProvider<String> LOCATION = StaticValueProvider.of("schemas.tar.gz");

  @Test
  public void testNumSchemas() {
    SchemaStore store = SchemaStore.of(LOCATION);
    // At time of writing this test, there were 47 schemas to load, but that number will
    // likely increase over time.
    assertThat(store.numLoadedSchemas(), greaterThan(40));
  }

  @Test
  public void testDocTypeExists() {
    SchemaStore store = SchemaStore.of(LOCATION);
    assertTrue(store.docTypeExists("telemetry", "main"));
    assertTrue(store.docTypeExists("telemetry", "update"));
  }

  @Test
  public void testDocTypeExistsViaAttributes() {
    SchemaStore store = SchemaStore.of(LOCATION);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "telemetry");
    attributes.put("document_type", "main");
    assertTrue(store.docTypeExists(attributes));
  }

}
