/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.mozilla.telemetry.decoder.AddMetadata;
import com.mozilla.telemetry.decoder.ParsePayload;
import com.mozilla.telemetry.decoder.ParseProxy;
import com.mozilla.telemetry.decoder.ParseUri;
import com.mozilla.telemetry.schemas.BigQuerySchemaStore;
import com.mozilla.telemetry.schemas.SchemaNotFoundException;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.SnakeCase;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
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

  public static PubsubMessageToTableRow of(ValueProvider<String> tableSpecTemplate,
      ValueProvider<List<String>> strictSchemaDocTypes, ValueProvider<String> schemasLocation,
      ValueProvider<String> schemasAliasesLocation, ValueProvider<TableRowFormat> tableRowFormat) {
    return new PubsubMessageToTableRow(tableSpecTemplate, strictSchemaDocTypes, schemasLocation,
        schemasAliasesLocation, tableRowFormat);
  }

  public enum TableRowFormat {
    raw, decoded, payload
  }

  public static final String PAYLOAD = "payload";
  public static final String ADDITIONAL_PROPERTIES = "additional_properties";
  public static final TimePartitioning TIME_PARTITIONING = new TimePartitioning()
      .setField(ParseProxy.SUBMISSION_TIMESTAMP);

  // We have hit rate limiting issues that have sent valid data to error output, so we make the
  // retry settings a bit more generous; see https://github.com/mozilla/gcp-ingestion/issues/651
  private static final RetrySettings RETRY_SETTINGS = ServiceOptions //
      .getDefaultRetrySettings().toBuilder() //
      .setMaxAttempts(12) // Defaults to 6
      .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(120)) // Defaults to 50 seconds
      .build();

  private final ValueProvider<String> tableSpecTemplate;
  private final ValueProvider<List<String>> strictSchemaDocTypes;
  private final ValueProvider<String> schemasLocation;
  private final ValueProvider<String> schemaAliasesLocation;
  private final ValueProvider<TableRowFormat> tableRowFormat;

  // We'll instantiate these on first use.
  private transient Cache<DatasetReference, Set<String>> tableListingCache;
  private transient Cache<TableReference, Schema> tableSchemaCache;
  private transient Cache<String, String> normalizedNameCache;
  private transient BigQuerySchemaStore schemaStore;
  private transient BigQuery bqService;

  private PubsubMessageToTableRow(ValueProvider<String> tableSpecTemplate,
      ValueProvider<List<String>> strictSchemaDocTypes, ValueProvider<String> schemasLocation,
      ValueProvider<String> schemaAliasesLocation, ValueProvider<TableRowFormat> tableRowFormat) {
    this.tableSpecTemplate = tableSpecTemplate;
    this.strictSchemaDocTypes = strictSchemaDocTypes;
    this.schemasLocation = schemasLocation;
    this.schemaAliasesLocation = schemaAliasesLocation;
    this.tableRowFormat = tableRowFormat;
  }

  @Override
  protected KV<TableDestination, TableRow> processElement(PubsubMessage message)
      throws IOException {
    message = PubsubConstraints.ensureNonNull(message);
    Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

    // We coerce all docType and namespace names to be snake_case and to remove invalid
    // characters; these transformations MUST match with the transformations applied by the
    // jsonschema-transpiler and mozilla-schema-generator when creating table schemas in BigQuery.
    final String namespace = attributes.get(ParseUri.DOCUMENT_NAMESPACE);
    final String docType = attributes.get(ParseUri.DOCUMENT_TYPE);
    if (namespace != null) {
      attributes.put(ParseUri.DOCUMENT_NAMESPACE, getAndCacheBqName(namespace));
    }
    if (docType != null) {
      attributes.put(ParseUri.DOCUMENT_TYPE, getAndCacheBqName(docType));
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
        TIME_PARTITIONING);
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
    } catch (ExecutionException e) {
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

    final TableRow tableRow;
    switch (tableRowFormat.get()) {
      case raw:
        tableRow = rawTableRow(message);
        break;
      case decoded:
        tableRow = decodedTableRow(message);
        break;
      case payload:
      default:
        tableRow = payloadTableRow(message, ref, tableSpec, namespace, docType, attributes);
        break;
    }
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
   * Turn the message into a TableRow that contains (likely gzipped) bytes as a "payload" field,
   * which is the format suitable for the "raw payload" tables in BigQuery that we use for
   * errors and recovery from pipeline failures.
   *
   * <p>We include all attributes as fields. It is up to the configured schema for the destination
   * table to determine which of those actually appear as fields; some of the attributes will be
   * thrown away.
   */
  @VisibleForTesting
  static TableRow rawTableRow(PubsubMessage message) {
    TableRow tableRow = new TableRow();
    message.getAttributeMap().forEach(tableRow::set);
    tableRow.set(PAYLOAD, message.getPayload());
    return tableRow;
  }

  /**
   * Like {@link #rawTableRow(PubsubMessage)}, but uses the nested metadata format of decoded pings.
   */
  @VisibleForTesting
  static TableRow decodedTableRow(PubsubMessage message) {
    TableRow tableRow = new TableRow();
    AddMetadata.attributesToMetadataPayload(message.getAttributeMap()).forEach(tableRow::set);
    // Also include client_id if present.
    Optional.ofNullable(message.getAttribute(ParsePayload.CLIENT_ID))
        .ifPresent(clientId -> tableRow.set(ParsePayload.CLIENT_ID, clientId));
    tableRow.set(PAYLOAD, message.getPayload());
    return tableRow;
  }

  private TableRow payloadTableRow(PubsubMessage message, TableReference ref, String tableSpec,
      String namespace, String docType, Map<String, String> attributes) throws IOException {

    if (schemaStore == null && schemasLocation != null && schemasLocation.isAccessible()
        && schemasLocation.get() != null) {
      schemaStore = BigQuerySchemaStore.of(schemasLocation, schemaAliasesLocation);
    }

    // If a schemasLocation is configured, we pull the table schema from there;
    // otherwise, we hit the BigQuery API to fetch the schema.
    Schema schema;
    if (schemaStore != null) {
      try {
        schema = schemaStore.getSchema(attributes);
      } catch (SchemaNotFoundException e) {
        throw new IllegalArgumentException(
            "The schema store does not contain a BigQuery schema for this table: " + tableSpec, e);
      }
    } else {
      if (tableSchemaCache == null) {
        tableSchemaCache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(1))
            .build();
      }
      try {
        schema = tableSchemaCache.get(ref, () -> {
          Table table = bqService.getTable(ref.getDatasetId(), ref.getTableId());
          if (table != null) {
            return table.getDefinition().getSchema();
          } else {
            return null;
          }
        });
      } catch (ExecutionException e) {
        throw new BubbleUpException(e.getCause());
      }
    }

    TableRow tableRow = Json.readTableRow(message.getPayload());

    // Strip metadata so that it's not subject to transformation.
    Object metadata = tableRow.remove(AddMetadata.METADATA);

    final boolean strictSchema = (strictSchemaDocTypes.isAccessible()
        && strictSchemaDocTypes.get().contains(String.format("%s/%s", namespace, docType)));

    // Make BQ-specific transformations to the payload structure.
    Map<String, Object> additionalProperties = strictSchema ? null : new HashMap<>();
    transformForBqSchema(tableRow, schema.getFields(), additionalProperties);

    tableRow.put(AddMetadata.METADATA, metadata);
    if (additionalProperties != null) {
      tableRow.put(ADDITIONAL_PROPERTIES, Json.asString(additionalProperties));
    }
    return tableRow;
  }

  /**
   * Recursively descend into the fields of the passed map and compare to the passed BQ schema,
   * while modifying the structure to accommodate map types, nested arrays, etc.
   *
   * @param parent the map object to inspect and transform
   * @param bqFields the list of expected BQ fields inside this object
   * @param additionalProperties a map for storing fields absent in the BQ schema; if null, this is
   *                             "strict schema" mode and additional properties will be dropped
   */
  public void transformForBqSchema(Map<String, Object> parent, List<Field> bqFields,
      Map<String, Object> additionalProperties) {
    Map<String, Field> bqFieldMap = bqFields.stream()
        .collect(Collectors.toMap(Field::getName, Function.identity()));

    HashSet<String> jsonFieldNames = new HashSet<>(parent.keySet());
    jsonFieldNames.forEach(jsonFieldName -> {
      final String bqFieldName;
      if (bqFieldMap.containsKey(jsonFieldName)) {
        // The JSON field name already matches a BQ field.
        bqFieldName = jsonFieldName;
      } else {
        // Try cleaning the name to match our BQ conventions.
        bqFieldName = getAndCacheBqName(jsonFieldName);

        // If the field name now matches a BQ field name, we rename the field within the payload,
        // otherwise we move it to additionalProperties without renaming.
        Object value = parent.remove(jsonFieldName);
        if (bqFieldMap.containsKey(bqFieldName)) {
          parent.put(bqFieldName, value);
        } else if (additionalProperties != null) {
          additionalProperties.put(jsonFieldName, value);
        }
      }

      Optional.ofNullable(bqFieldMap.get(bqFieldName))
          .ifPresent(field -> processField(jsonFieldName, field, parent.get(bqFieldName), parent,
              additionalProperties));
    });
  }

  /**
   * Return true if this field is a repeated struct of key/value, indicating a JSON map.
   */
  private static boolean isMapType(Field field) {
    return field.getType() == LegacySQLTypeName.RECORD && field.getMode() == Mode.REPEATED //
        && field.getSubFields().size() == 2 //
        && field.getSubFields().get(0).getName().equals("key") //
        && field.getSubFields().get(1).getName().equals("value");
  }

  private void processField(String jsonFieldName, Field field, Object val,
      Map<String, Object> parent, Map<String, Object> additionalProperties) {
    String name = field.getName();
    Optional<Object> value = Optional.ofNullable(val);

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
    } else if (isMapType(field)) {
      expandMapType(jsonFieldName, value.orElse(null), field, parent, additionalProperties);

      // We need to recursively call transformForBqSchema on any normal record type.
    } else if (field.getType() == LegacySQLTypeName.RECORD && field.getMode() != Mode.REPEATED) {
      value.filter(Map.class::isInstance).map(Map.class::cast).ifPresent(m -> {
        Map<String, Object> props = additionalProperties == null ? null : new HashMap<>();
        transformForBqSchema(m, field.getSubFields(), props);
        if (props != null && !props.isEmpty()) {
          additionalProperties.put(jsonFieldName, props);
        }
        parent.put(name, m);
      });

      // Likewise, we need to recursively call transformForBqSchema on repeated record types.
    } else if (field.getType() == LegacySQLTypeName.RECORD && field.getMode() == Mode.REPEATED) {
      List<Object> records = value.filter(List.class::isInstance).map(List.class::cast)
          .orElse(ImmutableList.of());
      records.stream().filter(Map.class::isInstance).map(Map.class::cast).forEach(record -> {
        Map<String, Object> props = additionalProperties == null ? null : new HashMap<>();
        transformForBqSchema(record, field.getSubFields(), props);
        if (props != null && !props.isEmpty()) {
          additionalProperties.put(name, props);
        }
      });
    } else {
      value.ifPresent(v -> parent.put(name, v));
    }
  }

  /**
   * Recursively descend into a map type field, expanding to the key/value struct required in
   * BigQuery schemas.
   */
  private void expandMapType(String jsonFieldName, Object val, Field field,
      Map<String, Object> parent, Map<String, Object> additionalProperties) {
    Optional<Object> value = Optional.ofNullable(val);
    value.filter(Map.class::isInstance).map(Map.class::cast).ifPresent(m -> {
      Map<String, Object> map = m;
      Field valueField = field.getSubFields().get(1);
      Map<String, Object> props = additionalProperties == null ? null : new HashMap<>();
      List<Map<String, Object>> unmapped = map.entrySet().stream().map(entry -> {
        Map<String, Object> kv = new HashMap<>(2);
        kv.put("key", entry.getKey());
        processField(entry.getKey(), valueField, entry.getValue(), kv, props);
        if (props != null && !props.isEmpty()) {
          additionalProperties.put(jsonFieldName, props);
        }
        return kv;
      }).collect(Collectors.toList());
      parent.put(field.getName(), unmapped);
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

  private String getAndCacheBqName(String name) {
    if (normalizedNameCache == null) {
      normalizedNameCache = CacheBuilder.newBuilder().maximumSize(50_000).build();
    }
    try {
      return normalizedNameCache.get(name, () -> convertNameForBq(name));
    } catch (ExecutionException e) {
      throw new BubbleUpException(e.getCause());
    }
  }

  /**
   * Converts a name to a BigQuery-friendly format.
   *
   * <p>The format must match exactly with the transformations made by jsonschema-transpiler
   * and mozilla-pipeline-schemas. In general, this format requires converting camelCase to
   * snake_case, replacing incompatible characters like '-' with underscores, and prepending
   * an underscore to names that begin with a digit.
   */
  @VisibleForTesting
  static String convertNameForBq(String name) {
    StringBuilder sb = new StringBuilder();
    if (name.length() > 0 && Character.isDigit(name.charAt(0))) {
      sb.append('_');
    }
    sb.append(SnakeCase.format(name));
    return sb.toString();
  }

}
