package com.mozilla.telemetry.ingestion.core.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.auto.value.AutoValue;
import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class PmdStore extends SchemaStore<PmdStore.PipelineMetadata> {

  @AutoValue
  @JsonDeserialize(builder = AutoValue_PmdStore_JweMapping.Builder.class)
  public abstract static class JweMapping {
    public static Builder builder() {
      return new AutoValue_PmdStore_JweMapping.Builder();
    }

    public abstract JsonPointer source_field_path();
    public abstract JsonPointer decrypted_field_path();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static class Builder {
      public abstract Builder source_field_path(JsonPointer value);
      public abstract Builder decrypted_field_path(JsonPointer value);

      public abstract JweMapping build();
    }

  }

  @AutoValue
  @JsonDeserialize(builder = AutoValue_PmdStore_PipelineMetadata.Builder.class)
  public abstract static class PipelineMetadata {
    public static Builder builder() {
      return new AutoValue_PmdStore_PipelineMetadata.Builder();
    }

    public abstract String bq_dataset_family();

    public abstract String bq_table();

    public abstract String bq_metadata_format();

    public abstract List<JweMapping> jwe_mappings();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static class Builder {
      public abstract Builder bq_dataset_family(String value);

      public abstract Builder bq_table(String value);

      public abstract Builder bq_metadata_format(String value);

      public abstract Builder jwe_mappings(List<JweMapping> value);

      public abstract PipelineMetadata build();
    }
  }

  @AutoValue
  @JsonDeserialize(builder = AutoValue_PmdStore_Container.Builder.class)
  public abstract static class Container {
    public static Builder builder() {
      return new AutoValue_PmdStore_Container.Builder();
    }

    @Nullable
    public abstract PipelineMetadata mozPipelineMetadata();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static class Builder {
      public abstract Builder mozPipelineMetadata(PipelineMetadata value);

      public abstract Container build();
    }
  }

  /**
   * Returns a SchemaStore based on the contents of the archive at schemasLocation.
   */
  public static PmdStore of(String schemasLocation,
      IOFunction<String, InputStream> open) {
    return new PmdStore(schemasLocation, open);
  }

  /**
   * Return a Schema from bytes for use outside of the SchemaStore.
   */
  public static PipelineMetadata readSchema(byte[] bytes) throws IOException {
    return Json.readValue(bytes, Container.class).mozPipelineMetadata();
  }

  protected PmdStore(String schemasLocation, IOFunction<String, InputStream> open) {
    super(schemasLocation, open);
  }

  @Override
  protected String schemaSuffix() {
    return ".schema.json";
  }

  @Override
  protected PipelineMetadata loadSchemaFromArchive(ArchiveInputStream archive) throws IOException {
    return readSchema(IOUtils.toByteArray(archive));
  }
}
