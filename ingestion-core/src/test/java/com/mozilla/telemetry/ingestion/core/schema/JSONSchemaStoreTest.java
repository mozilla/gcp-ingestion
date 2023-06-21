package com.mozilla.telemetry.ingestion.core.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.io.Resources;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.everit.json.schema.Schema;
import org.junit.Test;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JSONSchemaStoreTest {

  private static final JSONSchemaStore store = JSONSchemaStore.of(TestConstant.SCHEMAS_LOCATION,
      null);

  @Test
  public void testReadSchema() throws IOException {
    byte[] data = Resources.toByteArray(
        Resources.getResource("schema/test-schemas/schemas/namespace_0/foo/foo.1.schema.json"));
    Schema schema = JSONSchemaStore.readSchema(data);
    assertTrue(schema.definesProperty("payload"));
  }

  @Test
  public void testNumSchemas() {
    assertEquals(3, store.numLoadedSchemas());
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
    attributes.put("document_type", "bar");
    assertTrue(store.docTypeExists(attributes));
  }

  @Test
  public void testGetSchemaViaAttributes() throws SchemaNotFoundException {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "namespace_0");
    attributes.put("document_type", "foo");
    attributes.put("document_version", "1");
    Schema schema = store.getSchema(attributes);
    assertTrue(schema.definesProperty("payload"));

  }

  @Test
  public void testGetSchemaViaPath() throws SchemaNotFoundException {
    Schema schema = store.getSchema("namespace_0/foo/foo.1.schema.json");
    assertTrue(schema.definesProperty("payload"));
  }
}
