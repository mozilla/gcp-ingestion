/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoderV2;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.text.StringSubstitutor;

/**
 * Parses JSON payloads using Google's JSON API model library, emitting a BigQuery-specific
 * TableRow.
 *
 * <p>We also perform some light manipulation of the parsed JSON to match details of our table
 * schemas in BigQuery. In particular, we pull out submission_timestamp to a top level field so we
 * can use it as our partitioning field.
 */
public class PubsubMessageToTableRow
    extends MapElementsWithErrors<PubsubMessage, KV<TableDestination, TableRow>> {

  public static PubsubMessageToTableRow of(ValueProvider<String> tableSpecTemplate) {
    return new PubsubMessageToTableRow(tableSpecTemplate);
  }

  public static final String SUBMISSION_TIMESTAMP = "submission_timestamp";
  public static final TimePartitioning TIME_PARTITIONING = new TimePartitioning()
      .setField(SUBMISSION_TIMESTAMP);

  private final ValueProvider<String> tableSpecTemplate;

  // We'll instantiate this on first use.
  private transient Cache<DatasetReference, Set<String>> tableListingCache;

  private PubsubMessageToTableRow(ValueProvider<String> tableSpecTemplate) {
    this.tableSpecTemplate = tableSpecTemplate;
  }

  @Override
  protected KV<TableDestination, TableRow> processElement(PubsubMessage message)
      throws IOException {
    message = PubsubConstraints.ensureNonNull(message);
    // Only letters, numbers, and underscores are allowed in BigQuery dataset and table names,
    // but some doc types and namespaces contain '-', so we convert to '_'.
    final Map<String, String> attributes = Maps.transformValues(message.getAttributeMap(),
        v -> v.replaceAll("-", "_"));
    final String tableSpec = StringSubstitutor.replace(tableSpecTemplate.get(), attributes);

    // Send to error collection if incomplete tableSpec; $ is not a valid char in tableSpecs.
    if (tableSpec.contains("$")) {
      throw new IllegalArgumentException("Element did not contain all the attributes needed to"
          + " fill out variables in the configured BigQuery output template: "
          + tableSpecTemplate.get());
    }

    final TableDestination tableDestination = new TableDestination(tableSpec, null,
        TIME_PARTITIONING);
    final TableReference ref = BigQueryHelpers.parseTableSpec(tableSpec);
    final DatasetReference datasetRef = new DatasetReference().setProjectId(ref.getProjectId())
        .setDatasetId(ref.getDatasetId());

    // Get and cache a listing of table names for this dataset.
    Set<String> tablesInDataset;
    if (tableListingCache == null) {
      tableListingCache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(1)).build();
    }
    try {
      tablesInDataset = tableListingCache.get(datasetRef, () -> {
        Set<String> tableSet = new HashSet<>();
        BigQuery service = BigQueryOptions.newBuilder().setProjectId(ref.getProjectId()).build()
            .getService();
        Dataset dataset = service.getDataset(ref.getDatasetId());
        if (dataset != null) {
          dataset.list().iterateAll().forEach(t -> tableSet.add(t.getTableId().getTable()));
        }
        return tableSet;
      });
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e);
    }

    // Send to error collection if dataset or table doesn't exist so BigQueryIO doesn't throw a
    // pipeline execution exception.
    if (tablesInDataset.isEmpty()) {
      throw new IllegalArgumentException("Resolved destination dataset does not exist or has no "
          + " tables for tableSpec " + tableSpec);
    } else if (!tablesInDataset.contains(ref.getTableId())) {
      throw new IllegalArgumentException("Resolved destination table does not exist: " + tableSpec);
    }

    TableRow tableRow = buildTableRow(message.getPayload());
    return KV.of(tableDestination, tableRow);
  }

  @Override
  public WithErrors.Result<PCollection<KV<TableDestination, TableRow>>> expand(
      PCollection<PubsubMessage> input) {
    WithErrors.Result<PCollection<KV<TableDestination, TableRow>>> result = super.expand(input);
    result.output().setCoder(KvCoder.of(TableDestinationCoderV2.of(), TableRowJsonCoder.of()));
    return result;
  }

  @VisibleForTesting
  static TableRow buildTableRow(byte[] payload) throws IOException {
    TableRow tableRow = Json.readTableRow(payload);
    promoteSubmissionTimestamp(tableRow);
    return tableRow;
  }

  /**
   * BigQuery cannot partition tables by a nested field, so we promote submission_timestamp out of
   * metadata to a top-level field.
   */
  private static void promoteSubmissionTimestamp(TableRow tableRow) {
    Optional<Map> metadata = Optional.ofNullable(tableRow).map(row -> tableRow.get("metadata"))
        .filter(Map.class::isInstance).map(Map.class::cast);
    if (metadata.isPresent()) {
      Object submissionTimestamp = metadata.get().remove("submission_timestamp");
      if (submissionTimestamp instanceof String) {
        tableRow.putIfAbsent("submission_timestamp", submissionTimestamp);
      }
    }
  }

}
