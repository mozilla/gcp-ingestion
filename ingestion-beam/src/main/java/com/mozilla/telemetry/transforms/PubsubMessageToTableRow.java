/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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

  private final ValueProvider<String> tableSpecTemplate;
  private final Cache<TableId, Boolean> tableExistenceCache = CacheBuilder.newBuilder()
      .expireAfterWrite(Duration.ofMinutes(1)).build();

  // BigQuery is not Serializable, so we mark it transient and instantiate on first use.
  private transient BigQuery bigquery;

  private PubsubMessageToTableRow(ValueProvider<String> tableSpecTemplate) {
    this.tableSpecTemplate = tableSpecTemplate;
  }

  @Override
  protected KV<TableDestination, TableRow> processElement(PubsubMessage element)
      throws IOException {
    Map<String, String> attributes = Optional.ofNullable(element.getAttributeMap()) //
        // Only letters, numbers, and underscores are allowed in BigQuery dataset and table names,
        // but some doc types and namespaces contain '-', so we convert to '_'.
        .map(m -> Maps.transformValues(m, v -> v.replaceAll("-", "_"))).orElse(new HashMap<>());
    String tableSpec = StringSubstitutor.replace(tableSpecTemplate.get(), attributes);

    // Send to error collection if incomplete tableSpec; $ is not a valid char in tableSpecs.
    if (tableSpec.contains("$")) {
      throw new IllegalArgumentException("Element did not contain all the attributes needed to"
          + " fill out variables in the configured BigQuery output template: "
          + tableSpecTemplate.get());
    }

    if (bigquery == null) {
      bigquery = BigQueryOptions.getDefaultInstance().getService();
    }
    TableDestination tableDestination = new TableDestination(tableSpec, null);
    TableReference ref = BigQueryHelpers.parseTableSpec(tableDestination.getTableSpec());
    TableId tableId = TableId.of(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());

    // Send to error collection if table doesn't exist so BigQueryIO doesn't throw a pipeline
    // execution exception.
    try {
      // Get cached value if it exists, or call out to bigquery and cache the result.
      Boolean exists = tableExistenceCache.get(tableId, () -> bigquery.getTable(tableId) != null);
      if (!exists) {
        throw new IllegalArgumentException(
            "Resolved destination table does not exist: " + tableSpec);
      }
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e);
    }

    TableRow tableRow = buildTableRow(element.getPayload());
    return KV.of(tableDestination, tableRow);
  }

  @Override
  public ResultWithErrors<PCollection<KV<TableDestination, TableRow>>> expand(
      PCollection<? extends PubsubMessage> input) {
    ResultWithErrors<PCollection<KV<TableDestination, TableRow>>> result = super.expand(input);
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
