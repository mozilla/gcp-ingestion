package com.mozilla.telemetry.ingestion.core.schema;

import com.google.cloud.bigquery.Schema;
import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;

public class BigQuerySchemaStore extends SchemaStore<Schema> {

  /**
   * Returns a SchemaStore based on the contents of the archive at schemasLocation
   * with additional schemas aliased according to configuration.
   */
  public static BigQuerySchemaStore of(String schemasLocation, String schemaAliasesLocation,
      IOFunction<String, InputStream> open) {
    return new BigQuerySchemaStore(schemasLocation, schemaAliasesLocation, open);
  }

  protected BigQuerySchemaStore(String schemasLocation, String schemaAliasesLocation,
      IOFunction<String, InputStream> open) {
    super(schemasLocation, schemaAliasesLocation, open);
  }

  @Override
  protected String schemaSuffix() {
    return ".bq";
  }

  @Override
  protected Schema loadSchemaFromArchive(ArchiveInputStream archive) throws IOException {
    byte[] bytes = IOUtils.toByteArray(archive);
    return Json.readBigQuerySchema(bytes);
  }
}
