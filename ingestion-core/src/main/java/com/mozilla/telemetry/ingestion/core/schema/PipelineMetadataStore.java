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
  @JsonDeserialize(builder = AutoValue_PipelineMetadataStore_SplitConfigTarget.Builder.class)
  public abstract static class SplitConfigTarget {

    public static Builder builder() {
      return new AutoValue_PipelineMetadataStore_SplitConfigTarget.Builder();
    }

    public abstract String document_namespace();

    public abstract String document_type();

    public abstract String document_version();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static class Builder {

      public abstract Builder document_namespace(String value);

      public abstract Builder document_type(String value);

      public abstract Builder document_version(String value);

      public abstract SplitConfigTarget build();
    }
  }

  @AutoValue
  @JsonDeserialize(builder = AutoValue_PipelineMetadataStore_SplitConfig.Builder.class)
  public abstract static class SplitConfig {

    public static Builder builder() {
      return new AutoValue_PipelineMetadataStore_SplitConfig.Builder();
    }

    public abstract Boolean preserve_original();

    public abstract List<SplitConfigTarget> subsets();

    @Nullable
    public abstract SplitConfigTarget remainder();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static class Builder {

      public abstract Builder preserve_original(Boolean value);

      public abstract Builder subsets(List<SplitConfigTarget> value);

      public abstract Builder remainder(SplitConfigTarget value);

      public abstract SplitConfig build();
    }
  }

  @AutoValue
  @JsonDeserialize(builder = AutoValue_PipelineMetadataStore_PipelineMetadata.Builder.class)
  public abstract static class PipelineMetadata {

    public static Builder builder() {
      return new AutoValue_PipelineMetadataStore_PipelineMetadata.Builder();
    }

    @Nullable
    public abstract String bq_dataset_family();

    @Nullable
    public abstract String bq_table();

    @Nullable
    public abstract String bq_metadata_format();

    @Nullable
    public abstract String submission_timestamp_granularity();

    @Nullable
    public abstract List<AttributeOverride> override_attributes();

    @Nullable
    public abstract List<JweMapping> jwe_mappings();

    @Nullable
    public abstract ExpirationPolicy expiration_policy();

    @Nullable
    public abstract String sample_id_source_uuid_attribute();

    @Nullable
    public abstract List<String> sample_id_source_uuid_payload_path();

    @Nullable
    public abstract SplitConfig split_config();

    @Nullable
    public abstract Integer geoip_skip_entries();

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

      public abstract Builder sample_id_source_uuid_attribute(String value);

      public abstract Builder sample_id_source_uuid_payload_path(List<String> value);

      public abstract Builder split_config(SplitConfig value);

      public abstract Builder geoip_skip_entries(Integer value);

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
