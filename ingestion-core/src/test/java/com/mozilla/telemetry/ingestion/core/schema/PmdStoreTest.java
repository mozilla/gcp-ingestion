package com.mozilla.telemetry.ingestion.core.schema;

import static org.junit.Assert.assertEquals;

import com.mozilla.telemetry.ingestion.core.schema.PmdStore.PipelineMetadata;
import org.junit.Test;

public class PmdStoreTest {

  private static final PmdStore store = PmdStore
      .of(TestConstant.SCHEMAS_LOCATION, null);

  @Test
  public void testGetPipelineMetadata() throws SchemaNotFoundException {
    PipelineMetadata meta = store.getSchema("namespace_0/bar/bar.1.schema.json");
    assertEquals("namespace_0", meta.bq_dataset_family());
    assertEquals("/test_string", meta.jwe_mappings().get(0).decrypted_field_path().toString());
  }

}
