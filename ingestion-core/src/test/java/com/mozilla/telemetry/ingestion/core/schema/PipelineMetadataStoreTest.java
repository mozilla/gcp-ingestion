package com.mozilla.telemetry.ingestion.core.schema;

import static org.junit.Assert.assertEquals;

import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.PipelineMetadata;
import org.junit.Test;

public class PipelineMetadataStoreTest {

  private static final PipelineMetadataStore store = PipelineMetadataStore
      .of(TestConstant.SCHEMAS_LOCATION, null);

  @Test
  public void testGetPipelineMetadata() throws SchemaNotFoundException {
    PipelineMetadata meta = store.getSchema("namespace_0/bar/bar.1.schema.json");
    assertEquals("namespace_0", meta.bq_dataset_family());
    assertEquals("/test_string", meta.jwe_mappings().get(0).decrypted_field_path().toString());
  }

}
