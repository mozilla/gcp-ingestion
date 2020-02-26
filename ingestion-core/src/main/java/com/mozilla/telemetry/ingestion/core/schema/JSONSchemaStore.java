package com.mozilla.telemetry.ingestion.core.schema;

import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JSONSchemaStore extends SchemaStore<Schema> {

  /**
   * Returns a SchemaStore based on the contents of the archive at schemasLocation.
   */
  public static JSONSchemaStore of(String schemasLocation, IOFunction<String, InputStream> open) {
    return new JSONSchemaStore(schemasLocation, open);
  }

  protected JSONSchemaStore(String schemasLocation, IOFunction<String, InputStream> open) {
    super(schemasLocation, open);
  }

  @Override
  protected String schemaSuffix() {
    return ".schema.json";
  }

  @Override
  protected Schema loadSchemaFromArchive(ArchiveInputStream archive) throws IOException {
    byte[] bytes = IOUtils.toByteArray(archive);
    JSONObject json = Json.readValue(bytes, JSONObject.class);
    return SchemaLoader.load(json);
  }
}
