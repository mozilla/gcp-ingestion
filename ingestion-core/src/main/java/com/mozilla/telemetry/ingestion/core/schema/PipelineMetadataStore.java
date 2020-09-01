package com.mozilla.telemetry.ingestion.core.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.auto.value.AutoValue;
import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class PipelineMetadataStore extends SchemaStore<JsonNode> {

  @AutoValue
  public abstract static class PipelineMetadata {
    public static Builder builder() {
      return new AutoValue_PipelineMetadataStore_PipelineMetadata.Builder();
    }

    @JsonProperty("bq_dataset_family")
    public abstract String bqDatasetFamily();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    public abstract static class Builder {
      public abstract Builder bqDatasetFamily(String value);

      public abstract PipelineMetadata build();
    }
  }

  /**
   * Returns a SchemaStore based on the contents of the archive at schemasLocation.
   */
  public static PipelineMetadataStore of(String schemasLocation,
      IOFunction<String, InputStream> open) {
    return new PipelineMetadataStore(schemasLocation, open);
  }

  /**
   * Return a Schema from bytes for use outside of the SchemaStore.
   */
  public static JsonNode readSchema(byte[] bytes) throws IOException {
    return Json.readObjectNode(bytes).path("mozPipelineMetadata");
  }

  protected PipelineMetadataStore(String schemasLocation, IOFunction<String, InputStream> open) {
    super(schemasLocation, open);
  }

  @Override
  protected String schemaSuffix() {
    return ".schema.json";
  }

  @Override
  protected JsonNode loadSchemaFromArchive(ArchiveInputStream archive) throws IOException {
    return readSchema(IOUtils.toByteArray(archive));
  }
}
