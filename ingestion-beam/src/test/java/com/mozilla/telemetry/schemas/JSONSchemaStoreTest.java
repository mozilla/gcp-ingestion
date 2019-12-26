package com.mozilla.telemetry.schemas;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.everit.json.schema.Schema;
import org.junit.Test;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JSONSchemaStoreTest {

  private static final ValueProvider<String> SCHEMAS_LOCATION = StaticValueProvider
      .of("schemas.tar.gz");
  private static final ValueProvider<String> EMPTY_ALIASING_CONFIG_LOCATION = StaticValueProvider
      .of(null);
  private static final ValueProvider<String> ALIASING_CONFIG_LOCATION = StaticValueProvider
      .of("src/test/resources/schemaAliasing/example-aliasing-config.json");

  @Test
  public void testNumSchemas() {
    JSONSchemaStore store = JSONSchemaStore.of(SCHEMAS_LOCATION, EMPTY_ALIASING_CONFIG_LOCATION);
    // At time of writing this test, there were 47 schemas to load, but that number will
    // likely increase over time.
    assertThat(store.numLoadedSchemas(), greaterThan(40));
  }

  @Test
  public void testDocTypeExists() {
    JSONSchemaStore store = JSONSchemaStore.of(SCHEMAS_LOCATION, EMPTY_ALIASING_CONFIG_LOCATION);
    assertTrue(store.docTypeExists("telemetry", "main"));
    assertTrue(store.docTypeExists("telemetry", "update"));
    assertTrue(store.docTypeExists("telemetry", "untrustedModules"));
    assertTrue(store.docTypeExists("telemetry", "untrusted_modules"));
  }

  @Test
  public void testDocTypeExistsViaAttributes() {
    JSONSchemaStore store = JSONSchemaStore.of(SCHEMAS_LOCATION, EMPTY_ALIASING_CONFIG_LOCATION);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "telemetry");
    attributes.put("document_type", "main");
    assertTrue(store.docTypeExists(attributes));
  }

  @Test
  public void testAliasedDocTypeExists() {
    JSONSchemaStore store = JSONSchemaStore.of(SCHEMAS_LOCATION, ALIASING_CONFIG_LOCATION);
    assertTrue(store.docTypeExists("some-product", "baseline"));
    assertTrue(store.docTypeExists("some-product", "some-doctype"));
    assertTrue(store.docTypeExists("glean", "glean"));
  }

  @Test
  public void testAliasedSchemaExistsViaAttributes() throws SchemaNotFoundException {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "glean");
    attributes.put("document_type", "glean");
    attributes.put("document_version", "1");

    Map<String, String> aliasedAttributes = new HashMap<>();
    aliasedAttributes.put("document_namespace", "some-product");
    aliasedAttributes.put("document_type", "baseline");
    aliasedAttributes.put("document_version", "1");

    JSONSchemaStore store = JSONSchemaStore.of(SCHEMAS_LOCATION, ALIASING_CONFIG_LOCATION);
    Schema baseSchema = store.getSchema(attributes);
    Schema aliasedSchema = store.getSchema(aliasedAttributes);
    assertEquals(baseSchema, aliasedSchema);
  }

  @Test
  public void testGetSchemaViaAttributes() throws SchemaNotFoundException {
    JSONSchemaStore store = JSONSchemaStore.of(SCHEMAS_LOCATION, ALIASING_CONFIG_LOCATION);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "telemetry");
    attributes.put("document_type", "main");
    attributes.put("document_version", "4");
    Schema schema = store.getSchema(attributes);
    assertTrue(schema.definesProperty("clientId"));

  }

  @Test
  public void testGetSchemaViaPath() throws SchemaNotFoundException {
    JSONSchemaStore store = JSONSchemaStore.of(SCHEMAS_LOCATION, ALIASING_CONFIG_LOCATION);
    Schema schema = store.getSchema("telemetry/main/main.4.schema.json");
    assertTrue(schema.definesProperty("clientId"));
  }
}
