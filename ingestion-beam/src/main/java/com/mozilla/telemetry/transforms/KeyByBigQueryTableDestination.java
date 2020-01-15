package com.mozilla.telemetry.transforms;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.util.SnakeCase;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoderV3;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.text.StringSubstitutor;

public class KeyByBigQueryTableDestination
    extends MapElementsWithErrors<PubsubMessage, KV<TableDestination, PubsubMessage>> {

  public static KeyByBigQueryTableDestination of(ValueProvider<String> tableSpecTemplate,
      ValueProvider<String> partitioningField, ValueProvider<List<String>> clusteringFields) {
    return new KeyByBigQueryTableDestination(tableSpecTemplate, partitioningField,
        clusteringFields);
  }

  /**
   * Return the appropriate table desination instance for the given document type and other
   * attributes.
   */
  public TableDestination getTableDestination(Map<String, String> attributes) {
    attributes = new HashMap<>(attributes);

    // We coerce all docType and namespace names to be snake_case and to remove invalid
    // characters; these transformations MUST match with the transformations applied by the
    // jsonschema-transpiler and mozilla-schema-generator when creating table schemas in BigQuery.
    final String namespace = attributes.get(Attribute.DOCUMENT_NAMESPACE);
    final String docType = attributes.get(Attribute.DOCUMENT_TYPE);
    if (namespace != null) {
      attributes.put(Attribute.DOCUMENT_NAMESPACE, getAndCacheNormalizedName(namespace));
    }
    if (docType != null) {
      attributes.put(Attribute.DOCUMENT_TYPE, getAndCacheNormalizedName(docType));
    }

    // Only letters, numbers, and underscores are allowed in BigQuery dataset and table names,
    // but some doc types and namespaces contain '-', so we convert to '_'; we don't pass all
    // values through getAndCacheBqName to avoid expensive regex operations and polluting the
    // cache of transformed field names.
    attributes = Maps.transformValues(attributes, v -> v.replaceAll("-", "_"));

    final String tableSpec = StringSubstitutor.replace(tableSpecTemplate.get(), attributes);

    // Send to error collection if incomplete tableSpec; $ is not a valid char in tableSpecs.
    if (tableSpec.contains("$")) {
      throw new IllegalArgumentException("Element did not contain all the attributes needed to"
          + " fill out variables in the configured BigQuery output template: "
          + tableSpecTemplate.get());
    }

    final TableDestination tableDestination = new TableDestination(tableSpec, null,
        new TimePartitioning().setField(partitioningField.get()),
        new Clustering().setFields(clusteringFields.get()));
    final TableReference ref = BigQueryHelpers.parseTableSpec(tableSpec);
    final DatasetReference datasetRef = new DatasetReference().setProjectId(ref.getProjectId())
        .setDatasetId(ref.getDatasetId());

    if (bqService == null) {
      bqService = BigQueryOptions.newBuilder().setProjectId(ref.getProjectId())
          .setRetrySettings(RETRY_SETTINGS).build().getService();
    }

    // Get and cache a listing of table names for this dataset.
    Set<String> tablesInDataset;
    if (tableListingCache == null) {
      tableListingCache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(1)).build();
    }
    try {
      tablesInDataset = tableListingCache.get(datasetRef, () -> {
        Set<String> tableSet = new HashSet<>();
        Dataset dataset = bqService.getDataset(ref.getDatasetId());
        if (dataset != null) {
          dataset.list().iterateAll().forEach(t -> {
            tableSet.add(t.getTableId().getTable());
          });
        }
        return tableSet;
      });
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new BubbleUpException(e.getCause());
    }

    // Send to error collection if dataset or table doesn't exist so BigQueryIO doesn't throw a
    // pipeline execution exception.
    if (tablesInDataset.isEmpty()) {
      throw new IllegalArgumentException("Resolved destination dataset does not exist or has no "
          + " tables for tableSpec " + tableSpec);
    } else if (!tablesInDataset.contains(ref.getTableId())) {
      throw new IllegalArgumentException("Resolved destination table does not exist: " + tableSpec);
    }

    return tableDestination;
  }

  @Override
  protected KV<TableDestination, PubsubMessage> processElement(PubsubMessage message) {
    message = PubsubConstraints.ensureNonNull(message);

    return KV.of(getTableDestination(message.getAttributeMap()), message);
  }

  @Override
  public WithErrors.Result<PCollection<KV<TableDestination, PubsubMessage>>> expand(
      PCollection<PubsubMessage> input) {
    WithErrors.Result<PCollection<KV<TableDestination, PubsubMessage>>> result = super.expand(
        input);
    result.output().setCoder(KvCoder.of(TableDestinationCoderV3.of(),
        PubsubMessageWithAttributesAndMessageIdCoder.of()));
    return result;
  }

  ////

  private final ValueProvider<String> tableSpecTemplate;
  private final ValueProvider<String> partitioningField;
  private final ValueProvider<List<String>> clusteringFields;

  // We'll instantiate these on first use.
  private transient Cache<DatasetReference, Set<String>> tableListingCache;
  private transient Cache<String, String> normalizedNameCache;
  private transient BigQuery bqService;

  // We have hit rate limiting issues that have sent valid data to error output, so we make the
  // retry settings a bit more generous; see https://github.com/mozilla/gcp-ingestion/issues/651
  private static final RetrySettings RETRY_SETTINGS = ServiceOptions //
      .getDefaultRetrySettings().toBuilder() //
      .setMaxAttempts(12) // Defaults to 6
      .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(120)) // Defaults to 50 seconds
      .build();

  private KeyByBigQueryTableDestination(ValueProvider<String> tableSpecTemplate,
      ValueProvider<String> partitioningField, ValueProvider<List<String>> clusteringFields) {
    this.tableSpecTemplate = tableSpecTemplate;
    this.partitioningField = partitioningField;
    this.clusteringFields = clusteringFields;
  }

  private String getAndCacheNormalizedName(String name) {
    if (normalizedNameCache == null) {
      normalizedNameCache = CacheBuilder.newBuilder().maximumSize(50_000).build();
    }
    try {
      return normalizedNameCache.get(name, () -> SnakeCase.format(name));
    } catch (ExecutionException e) {
      throw new BubbleUpException(e.getCause());
    }
  }

}
