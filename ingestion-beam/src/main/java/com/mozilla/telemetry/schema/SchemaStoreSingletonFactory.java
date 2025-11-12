package com.mozilla.telemetry.schema;

import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore;
import com.mozilla.telemetry.util.BeamFileInputStream;

/**
 * Factory class for getting singleton instances of SchemaStores.
 *
 * <p>Beam can run up to 500 threads per worker
 * (see https://cloud.google.com/dataflow/docs/guides/troubleshoot-oom#dofn)
 * By using singleton instances of SchemaStores we can speed up worker startup a bit and
 * decrease memory utilization.
 *
 * <p>`schemasLocation` parameter in the factory methods here is used only during object creation.
 * This is fine here since the value is not changing during runtime of the job.
 */
public class SchemaStoreSingletonFactory {

  private static volatile JSONSchemaStore jsonSchemaStore;
  private static volatile PipelineMetadataStore pipelineMetadataStore;

  private SchemaStoreSingletonFactory() {
  }

  @VisibleForTesting
  public static synchronized void clearSingletonsForTests() {
    jsonSchemaStore = null;
    pipelineMetadataStore = null;
  }

  /**
   * Returns a singleton instance of {@link JSONSchemaStore}.
   */
  public static JSONSchemaStore getJsonSchemaStore(String schemasLocation) {
    if (jsonSchemaStore == null) {
      synchronized (SchemaStoreSingletonFactory.class) {
        if (jsonSchemaStore == null) {
          jsonSchemaStore = JSONSchemaStore.of(schemasLocation, BeamFileInputStream::open);
        }
      }
    }
    return jsonSchemaStore;
  }

  /**
   * Returns a singleton instance of {@link PipelineMetadataStore}.
   */
  public static PipelineMetadataStore getPipelineMetadataStore(String schemasLocation) {
    if (pipelineMetadataStore == null) {
      synchronized (SchemaStoreSingletonFactory.class) {
        if (pipelineMetadataStore == null) {
          pipelineMetadataStore = PipelineMetadataStore.of(schemasLocation,
              BeamFileInputStream::open);
        }
      }
    }
    return pipelineMetadataStore;
  }

}
