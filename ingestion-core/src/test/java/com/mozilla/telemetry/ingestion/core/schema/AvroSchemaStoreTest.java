package com.mozilla.telemetry.ingestion.core.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.junit.Test;

public class AvroSchemaStoreTest {

  private static final AvroSchemaStore store = AvroSchemaStore.of(TestConstant.SCHEMAS_LOCATION,
      null, null);

  @Test
  public void testNumSchemas() {
    assertEquals(store.numLoadedSchemas(), 3);
  }

  @Test
  public void testDocTypeExists() {
    assertTrue(store.docTypeExists("namespace_0", "foo"));
    assertTrue(store.docTypeExists("namespace_0", "bar"));
    assertTrue(store.docTypeExists("namespace_1", "baz"));
  }

  @Test
  public void testDocTypeExistsViaAttributes() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "namespace_0");
    attributes.put("document_type", "foo");
    assertTrue(store.docTypeExists(attributes));
  }

  @Test
  public void testGetSchemaViaAttributes() throws SchemaNotFoundException {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "namespace_0");
    attributes.put("document_type", "foo");
    attributes.put("document_version", "1");
    Schema schema = store.getSchema(attributes);
    assertEquals(schema.getField("test_int").schema().getType(), Schema.Type.INT);

  }

  @Test
  public void testGetSchemaViaPath() throws SchemaNotFoundException {
    Schema schema = store.getSchema("namespace_0/foo/foo.1.avro.json");
    assertEquals(schema.getField("test_int").schema().getType(), Schema.Type.INT);
  }
}
