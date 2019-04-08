/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.schemas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.io.Resources;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;

public class AvroSchemaStoreTest {

  /**
    avro-schema-test/
  └── schemas
      ├── namespace_0
      │   ├── bar
      │   │   └── bar.1.avro.json
      │   └── foo
      │       └── foo.1.avro.json
      └── namespace_1
          └── baz
              ├── baz.1.avro.json
              └── baz.1.schema.json
  6 directories, 4 files
  */
  private static final ValueProvider<String> LOCATION = StaticValueProvider
      .of(Resources.getResource("avro/test-schema.tar.gz").getPath());

  @Test
  public void testNumSchemas() {
    AvroSchemaStore store = AvroSchemaStore.of(LOCATION);
    assertEquals(store.numLoadedSchemas(), 3);
  }

  @Test
  public void testDocTypeExists() {
    AvroSchemaStore store = AvroSchemaStore.of(LOCATION);
    assertTrue(store.docTypeExists("namespace_0", "foo"));
    assertTrue(store.docTypeExists("namespace_0", "bar"));
    assertTrue(store.docTypeExists("namespace_1", "baz"));
  }

  @Test
  public void testDocTypeExistsViaAttributes() {
    AvroSchemaStore store = AvroSchemaStore.of(LOCATION);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "namespace_0");
    attributes.put("document_type", "foo");
    assertTrue(store.docTypeExists(attributes));
  }

}
