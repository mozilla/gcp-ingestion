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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mozilla.telemetry.decoder.AddMetadata;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
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
 * <p>We also perform some manipulation of the parsed JSON to match details of our table
 * schemas in BigQuery.
 */
public class PubsubMessageToTableRow
    extends MapElementsWithErrors<PubsubMessage, KV<TableDestination, TableRow>> {

  public static PubsubMessageToTableRow of(ValueProvider<String> tableSpecTemplate) {
    return new PubsubMessageToTableRow(tableSpecTemplate);
  }

  public static final String SUBMISSION_TIMESTAMP = "submission_timestamp";
  public static final String ADDITIONAL_PROPERTIES = "additional_properties";
  public static final TimePartitioning TIME_PARTITIONING = new TimePartitioning()
      .setField(SUBMISSION_TIMESTAMP);

  private final ValueProvider<String> tableSpecTemplate;

  // We'll instantiate these on first use.
  private transient Cache<DatasetReference, Set<String>> tableListingCache;
  private transient Cache<TableReference, Schema> tableSchemaCache;

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
          dataset.list().iterateAll().forEach(t -> {
            tableSet.add(t.getTableId().getTable());
          });
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

    // Get and cache the BQ schema for this table.
    Schema schema = null;
    if (tableSchemaCache == null) {
      tableSchemaCache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(1)).build();
    }
    try {
      schema = tableSchemaCache.get(ref, () -> {
        BigQuery service = BigQueryOptions.newBuilder().setProjectId(ref.getProjectId()).build()
            .getService();
        Table table = service.getTable(ref.getDatasetId(), ref.getTableId());
        if (table != null) {
          return table.getDefinition().getSchema();
        } else {
          return null;
        }
      });
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e);
    }

    TableRow tableRow = Json.readTableRow(message.getPayload());

    // Strip metadata so that it's not subject to transformation.
    Object metadata = tableRow.remove(AddMetadata.METADATA);

    // Make BQ-specific transformations to the payload structure.
    Map<String, Object> additionalProperties = new HashMap<>();
    transformForBqSchema(tableRow, schema.getFields(), additionalProperties);

    tableRow.put(AddMetadata.METADATA, metadata);
    tableRow.put(ADDITIONAL_PROPERTIES, Json.asString(additionalProperties));
    return KV.of(tableDestination, tableRow);
  }

  @Override
  public WithErrors.Result<PCollection<KV<TableDestination, TableRow>>> expand(
      PCollection<PubsubMessage> input) {
    WithErrors.Result<PCollection<KV<TableDestination, TableRow>>> result = super.expand(input);
    result.output().setCoder(KvCoder.of(TableDestinationCoderV2.of(), TableRowJsonCoder.of()));
    return result;
  }

  /**
   * Recursively descend into the fields of the passed map and compare to the passed BQ schema,
   * while modifying the structure to accommodate map types, nested arrays, etc.
   *
   * @param parent the map object to inspect and transform
   * @param bqFields the list of expected BQ fields inside this object
   * @param additionalProperties a map for storing fields absent in the BQ schema
   */
  public static void transformForBqSchema(Map<String, Object> parent, List<Field> bqFields,
      Map<String, Object> additionalProperties) {

    // Clean the key names.
    ImmutableSet.copyOf(parent.keySet()).forEach(rawKey -> {
      String key = rawKey;
      if (key.contains(".") || key.contains("-")) {
        key = key.replace(".", "_").replace("-", "_");
      }
      if (Character.isDigit(key.charAt(0))) {
        key = "_" + key;
      }
      parent.put(key, parent.remove(rawKey));
    });

    // Populate additionalProperties.
    Set<String> fieldNames = bqFields.stream().map(Field::getName).collect(Collectors.toSet());
    ImmutableSet.copyOf(Sets.difference(parent.keySet(), fieldNames))
        .forEach(k -> additionalProperties.put(k, parent.remove(k)));

    // Special transformations for structures disallowed in BigQuery.
    bqFields.forEach(field -> {
      String name = field.getName();
      Optional<Object> value = Optional.ofNullable(parent.get(name));

      // A repeated string field might need us to JSON-ify a list or map.
      if (field.getType() == LegacySQLTypeName.STRING && field.getMode() == Mode.REPEATED) {

        value.filter(List.class::isInstance).map(List.class::cast).ifPresent(list -> {
          List<Object> jsonified = ((List<Object>) list).stream().map(o -> coerceToString(o))
              .collect(Collectors.toList());
          parent.put(name, jsonified);
        });

        // A string field might need us to JSON-ify an object coerce a value to string.
      } else if (field.getType() == LegacySQLTypeName.STRING && field.getMode() != Mode.REPEATED) {
        value.ifPresent(o -> parent.put(name, coerceToString(o)));

        // A record of key and value indicates we need to transformForBqSchema a map to an array.
      } else if (field.getType() == LegacySQLTypeName.RECORD && field.getMode() == Mode.REPEATED //
          && field.getSubFields().size() == 2 //
          && field.getSubFields().get(0).getName().equals("key") //
          && field.getSubFields().get(1).getName().equals("value")) {
        value.filter(Map.class::isInstance).map(Map.class::cast).ifPresent(m -> {
          Map<String, Object> map = m;
          Field valueField = field.getSubFields().get(1);
          if (valueField.getType() == LegacySQLTypeName.RECORD) {
            Map<String, Object> props = new HashMap<>();
            transformForBqSchema(map, valueField.getSubFields(), props);
            if (!props.isEmpty()) {
              additionalProperties.put(name, props);
            }
          }
          List<Map<String, Object>> unmapped = map.entrySet().stream()
              .map(entry -> ImmutableMap.of("key", entry.getKey(), "value",
                  coerceIfStringExpected(entry.getValue(), valueField.getType())))
              .collect(Collectors.toList());
          parent.put(name, unmapped);
        });

        // We need to recursively call transformForBqSchema on any normal record type.
      } else if (field.getType() == LegacySQLTypeName.RECORD && field.getMode() != Mode.REPEATED) {
        value.filter(Map.class::isInstance).map(Map.class::cast).ifPresent(m -> {
          Map<String, Object> props = new HashMap<>();
          transformForBqSchema(m, field.getSubFields(), props);
          if (!props.isEmpty()) {
            additionalProperties.put(name, props);
          }
        });

        // Likewise, we need to recursively call transformForBqSchema on repeated record types.
      } else if (field.getType() == LegacySQLTypeName.RECORD && field.getMode() == Mode.REPEATED) {
        List<Object> records = value.filter(List.class::isInstance).map(List.class::cast)
            .orElse(ImmutableList.of());
        records.stream().filter(Map.class::isInstance).map(Map.class::cast).forEach(record -> {
          Map<String, Object> props = new HashMap<>();
          transformForBqSchema(record, field.getSubFields(), props);
          if (!props.isEmpty()) {
            additionalProperties.put(name, props);
          }
        });
      }
    });
  }

  private static String coerceToString(Object o) {
    if (o instanceof String) {
      return (String) o;
    } else {
      try {
        return Json.asString(o);
      } catch (IOException ignore) {
        return o.toString();
      }
    }
  }

  private static Object coerceIfStringExpected(Object o, LegacySQLTypeName typeName) {
    if (typeName == LegacySQLTypeName.STRING) {
      return coerceToString(o);
    } else {
      return o;
    }
  }

}
