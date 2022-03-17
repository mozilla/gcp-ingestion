package com.mozilla.telemetry.ingestion.core.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonPointer;
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

/**
 * This schema store uses the same mozilla-pipeline-schemas tarball as JSONSchemaStore,
 * but instead of returning Schema instances to be used for validation, it returns
 * {@link PipelineMetadata} objects parsed from each schema's "mozPipelineMetadata" entry.
 */
public class PipelineMetadataStore extends SchemaStore<PipelineMetadataStore.PipelineMetadata> {

  @AutoValue
  @JsonDeserialize(builder = AutoValue_PipelineMetadataStore_JweMapping.Builder.class)
  public abstract static class JweMapping {

    public static Builder builder() {
      return new AutoValue_PipelineMetadataStore_JweMapping.Builder();
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
  @JsonDeserialize(builder = AutoValue_PipelineMetadataStore_AttributeOverride.Builder.class)
  public abstract static class AttributeOverride {

    public static Builder builder() {
      return new AutoValue_PipelineMetadataStore_AttributeOverride.Builder();
    }

    public abstract String name();

    @Nullable
    public abstract String value();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static class Builder {

      public abstract Builder name(String value);

      public abstract Builder value(String value);

      public abstract AttributeOverride build();
    }

  }

  @AutoValue
  @JsonDeserialize(builder = AutoValue_PipelineMetadataStore_ExpirationPolicy.Builder.class)
  public abstract static class ExpirationPolicy {

    public static Builder builder() {
      return new AutoValue_PipelineMetadataStore_ExpirationPolicy.Builder();
    }

    @Nullable
    public abstract Integer delete_after_days();

    @Nullable
    public abstract String collect_through_date();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static class Builder {

      public abstract Builder delete_after_days(Integer value);

      public abstract Builder collect_through_date(String value);

      public abstract ExpirationPolicy build();
    }

  }

  @AutoValue
  @JsonDeserialize(builder = AutoValue_PipelineMetadataStore_PipelineMetadata.Builder.class)
  public abstract static class PipelineMetadata {

    public static Builder builder() {
      return new AutoValue_PipelineMetadataStore_PipelineMetadata.Builder();
    }

    public abstract String bq_dataset_family();

    public abstract String bq_table();

    public abstract String bq_metadata_format();

    @Nullable
    public abstract String submission_timestamp_granularity();

    @Nullable
    public abstract List<AttributeOverride> override_attributes();

    @Nullable
    public abstract List<JweMapping> jwe_mappings();

    @Nullable
    public abstract ExpirationPolicy expiration_policy();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static class Builder {

      public abstract Builder bq_dataset_family(String value);

      public abstract Builder bq_table(String value);

      public abstract Builder bq_metadata_format(String value);

      public abstract Builder submission_timestamp_granularity(String value);

      public abstract Builder override_attributes(List<AttributeOverride> value);

      public abstract Builder jwe_mappings(List<JweMapping> value);

      public abstract Builder expiration_policy(ExpirationPolicy value);

      public abstract PipelineMetadata build();
    }
  }

  @AutoValue
  @JsonDeserialize(builder = AutoValue_PipelineMetadataStore_Container.Builder.class)
  public abstract static class Container {

    public static Builder builder() {
      return new AutoValue_PipelineMetadataStore_Container.Builder();
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
  public static PipelineMetadataStore of(String schemasLocation,
      IOFunction<String, InputStream> open) {
    return new PipelineMetadataStore(schemasLocation, open);
  }

  /**
   * Return a Schema from bytes for use outside of the SchemaStore.
   */
  public static PipelineMetadata readSchema(byte[] bytes) throws IOException {
    return Json.readValue(bytes, Container.class).mozPipelineMetadata();
  }

  protected PipelineMetadataStore(String schemasLocation, IOFunction<String, InputStream> open) {
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
