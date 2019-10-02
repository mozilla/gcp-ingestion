package com.mozilla.telemetry.schemas;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.compress.archivers.ArchiveInputStream;

public class AvroSchemaStore extends SchemaStore<Schema> {

  /** Returns a SchemaStore based on the contents of the archive at schemasLocation. */
  public static AvroSchemaStore of(ValueProvider<String> schemasLocation,
      ValueProvider<String> schemaAliasesLocation) {
    return new AvroSchemaStore(schemasLocation, schemaAliasesLocation);
  }

  protected AvroSchemaStore(ValueProvider<String> schemasLocation,
      ValueProvider<String> schemaAliasesLocation) {
    super(schemasLocation, schemaAliasesLocation);
  }

  @Override
  protected String schemaSuffix() {
    return ".avro.json";
  }

  @Override
  protected Schema loadSchemaFromArchive(ArchiveInputStream archive) throws IOException {
    Schema.Parser parser = new Schema.Parser();
    return parser.parse(archive);
  }
}
