package com.mozilla.telemetry.ingestion.core.schema;

import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.commons.compress.archivers.ArchiveInputStream;

public class AvroSchemaStore extends SchemaStore<Schema> {

  /** Returns a SchemaStore based on the contents of the archive at schemasLocation. */
  public static AvroSchemaStore of(String schemasLocation, IOFunction<String, InputStream> open) {
    return new AvroSchemaStore(schemasLocation, open);
  }

  protected AvroSchemaStore(String schemasLocation, IOFunction<String, InputStream> open) {
    super(schemasLocation, open);
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
