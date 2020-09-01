package com.mozilla.telemetry.ingestion.core.schema;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

public class PipelineMetadataStoreTest {

  private static final PipelineMetadataStore store = PipelineMetadataStore
      .of(TestConstant.SCHEMAS_LOCATION, null);

  @Test
  public void testGetPipelineMetadata() throws SchemaNotFoundException {
    JsonNode meta = store.getSchema("namespace_0/bar/bar.1.schema.json");
    assertEquals("namespace_0", meta.path("bq_dataset_family").textValue());
    assertEquals("bar_v1", meta.path("bq_table").textValue());
    assertEquals(1, meta.path("jwe_mappings").size());
  }

}
