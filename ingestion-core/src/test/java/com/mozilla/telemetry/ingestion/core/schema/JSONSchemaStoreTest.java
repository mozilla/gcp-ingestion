package com.mozilla.telemetry.ingestion.core.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.everit.json.schema.Schema;
import org.junit.Test;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JSONSchemaStoreTest {

  private static final JSONSchemaStore store = JSONSchemaStore.of(Constant.SCHEMAS_LOCATION,
      Constant.SCHEMA_ALIASES_LOCATION, null);

  @Test
  public void testNumSchemas() {
    assertEquals(5, store.numLoadedSchemas());
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
  public void testAliasedDocTypeExists() {
    assertTrue(store.docTypeExists("some-product", "baseline"));
    assertTrue(store.docTypeExists("some-product", "some-doctype"));
    assertTrue(store.docTypeExists("namespace_1", "baz"));
  }

  @Test
  public void testAliasedSchemaExistsViaAttributes() throws SchemaNotFoundException {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "namespace_1");
    attributes.put("document_type", "baz");
    attributes.put("document_version", "1");

    Map<String, String> aliasedAttributes = new HashMap<>();
    aliasedAttributes.put("document_namespace", "some-product");
    aliasedAttributes.put("document_type", "baseline");
    aliasedAttributes.put("document_version", "1");

    Schema baseSchema = store.getSchema(attributes);
    Schema aliasedSchema = store.getSchema(aliasedAttributes);
    assertEquals(baseSchema, aliasedSchema);
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
