package com.mozilla.telemetry.schemas;

import com.google.cloud.bigquery.Schema;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;

public class BigQuerySchemaStore extends SchemaStore<Schema> {

  /**
   * Returns a SchemaStore based on the contents of the archive at schemasLocation
   * with additional schemas aliased according to configuration.
   */
  public static BigQuerySchemaStore of(ValueProvider<String> schemasLocation,
      ValueProvider<String> schemaAliasesLocation) {
    return new BigQuerySchemaStore(schemasLocation, schemaAliasesLocation);
  }

  protected BigQuerySchemaStore(ValueProvider<String> schemasLocation,
      ValueProvider<String> schemaAliasesLocation) {
    super(schemasLocation, schemaAliasesLocation);
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
