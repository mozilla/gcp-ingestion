package com.mozilla.telemetry.ingestion.core.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.PipelineMetadata;
import org.junit.Test;

public class PipelineMetadataStoreTest {

  private static final PipelineMetadataStore store = PipelineMetadataStore
      .of(TestConstant.SCHEMAS_LOCATION, null);

  @Test
  public void testGetPipelineMetadata() throws SchemaNotFoundException {
    final PipelineMetadata meta = store.getSchema("namespace_0/bar/bar.1.schema.json");
    assertEquals("namespace_0", meta.bq_dataset_family());
    assertEquals("/test_string", meta.jwe_mappings().get(0).decrypted_field_path().toString());
    assertEquals("document_id", meta.sample_id_source_uuid_attribute());
    assertEquals(ImmutableList.of("impression_id"), meta.sample_id_source_uuid_payload_path());
    assertNull(meta.split_config());
  }

  @Test
  public void testExpirationMetadata() throws SchemaNotFoundException {
    final PipelineMetadata meta = store.getSchema("namespace_0/bar/bar.1.schema.json");
    assertEquals("2022/03/01", meta.expiration_policy().collect_through_date());
  }

  @Test
  public void testGetSplitConfig() throws SchemaNotFoundException {
    final PipelineMetadata preserve = store.getSchema("test_split/preserve/preserve.1.schema.json");
    assertEquals(true, preserve.split_config().preserve_original());
    assertEquals("test_split", preserve.split_config().remainder().document_namespace());
    assertEquals("remainder", preserve.split_config().remainder().document_type());
    assertEquals("1", preserve.split_config().remainder().document_version());
    assertEquals(1, preserve.split_config().subsets().size());
    assertEquals("test_split", preserve.split_config().subsets().get(0).document_namespace());
    assertEquals("subset", preserve.split_config().subsets().get(0).document_type());
    assertEquals("1", preserve.split_config().subsets().get(0).document_version());

    final PipelineMetadata destroy = store.getSchema("test_split/destroy/destroy.1.schema.json");
    assertEquals(false, destroy.split_config().preserve_original());
    assertNull(destroy.split_config().remainder());
    assertEquals(1, destroy.split_config().subsets().size());
    assertEquals("test_split", destroy.split_config().subsets().get(0).document_namespace());
    assertEquals("subset", destroy.split_config().subsets().get(0).document_type());
    assertEquals("1", destroy.split_config().subsets().get(0).document_version());
  }
}
