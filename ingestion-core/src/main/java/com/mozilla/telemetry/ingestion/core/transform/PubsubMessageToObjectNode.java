package com.mozilla.telemetry.ingestion.core.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.Constant.FieldName;
import com.mozilla.telemetry.ingestion.core.schema.BigQuerySchemaStore;
import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import com.mozilla.telemetry.ingestion.core.util.Json;
import com.mozilla.telemetry.ingestion.core.util.SnakeCase;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.View.Name;
import io.opencensus.stats.ViewManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Transform attributes and data into an {@link ObjectNode}.
 */
public abstract class PubsubMessageToObjectNode {

  private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
  private static final ObjectNode EMPTY_OBJECT = Json.createObjectNode();

  public abstract ObjectNode apply(TableId tableId, Map<String, String> attributes, byte[] data);

  @VisibleForTesting
  ObjectNode apply(Map<String, String> attributes, byte[] data) {
    return apply(null, attributes, data);
  }

  public static class Raw extends PubsubMessageToObjectNode {

    private static final Raw INSTANCE = new Raw();

    public static Raw of() {
      return INSTANCE;
    }

    /**
     * Turn message into an {@link ObjectNode} that contains (likely gzipped) bytes as a "payload"
     * field, which is the format suitable for the "raw payload" tables in BigQuery that we use for
     * errors and recovery from pipeline failures.
     *
     * <p>We include all attributes as fields. It is up to the configured schema for the destination
     * table to determine which of those actually appear as fields; some of the attributes may be
     * thrown away.
     */
    @Override
    public ObjectNode apply(TableId tableId, Map<String, String> attributes, byte[] data) {
      ObjectNode contents = Json.asObjectNode(attributes);
      // bytes must be formatted as base64 encoded string.
      Optional.of(BASE64_ENCODER.encodeToString(data))
          // include payload if present.
          .filter(payload -> !payload.isEmpty())
          .ifPresent(payload -> contents.put(FieldName.PAYLOAD, payload));
      return contents;
    }
  }

  public static class Decoded extends PubsubMessageToObjectNode {

    private static final Decoded INSTANCE = new Decoded();

    public static Decoded of() {
      return INSTANCE;
    }

    /**
     * Like {@link Raw}, but uses the nested metadata format of decoded pings.
     *
     * <p>We include most attributes as fields, but the nested metadata format does not include
     * error attributes and only includes {@code metadata.uri} when document namespace is
     * {@code "telemetry"}.
     */
    @Override
    public ObjectNode apply(TableId tableId, Map<String, String> attributes, byte[] data) {
      ObjectNode contents = Json.asObjectNode(AddMetadata.attributesToMetadataPayload(attributes));
      // bytes must be formatted as base64 encoded string.
      Optional.of(BASE64_ENCODER.encodeToString(data))
          // include payload if present.
          .filter(payload -> !payload.isEmpty())
          .ifPresent(payload -> contents.put(FieldName.PAYLOAD, payload));
      // Also include client_id if present.
      Optional.ofNullable(attributes.getOrDefault(Attribute.CLIENT_ID, null))
          .ifPresent(clientId -> contents.put(Attribute.CLIENT_ID, clientId));
      return contents;
    }
  }

  public static class Payload extends PubsubMessageToObjectNode {

    public static class WithOpenCensusMetrics extends Payload {

      /**
       * Measure transform with OpenCensus metrics.
       */
      private WithOpenCensusMetrics(Cache<String, String> normalizedNameCache,
          Predicate<Map<String, String>> strictSchema, BigQuerySchemaStore schemaStore) {
        super(normalizedNameCache, strictSchema, schemaStore);
        setupOpenCensus();
      }

      private static final MeasureLong COERCED_TO_INT = MeasureLong.create("coerced_to_int",
          "The number of values coerced to integers", "1");
      private static final MeasureLong NOT_COERCED_TO_INT = MeasureLong.create("not_coerced_to_int",
          "The number of values that failed to be coerced to int", "1");
      private static final MeasureLong NOT_COERCED_TO_BOOL = MeasureLong.create(
          "not_coerced_to_bool", "The number of values that failed to be coerced to bool", "1");
      private static final MeasureLong INVALID_HISTOGRAM_TYPE = MeasureLong.create(
          "invalid_histogram_type", //
          "The number of parsed histograms with an invalid histogram_type", "1");
      private static final MeasureLong INVALID_HISTOGRAM_SUM = MeasureLong.create(
          "invalid_histogram_sum", //
          "The number of parsed histograms with an invalid sum field", "1");
      private static final MeasureLong INVALID_HISTOGRAM_USE_COUNTER = MeasureLong.create(
          "invalid_histogram_use_counter", //
          "The number of parsed histograms failing use counter invariants", "1");
      private static final MeasureLong INVALID_HISTOGRAM_RANGE = MeasureLong.create(
          "invalid_histogram_range", //
          "The number of parsed histograms with an invalid bucket_count or range field", "1");
      private static final Aggregation.Count COUNT_AGGREGATION = Aggregation.Count.create();
      private static final StatsRecorder STATS_RECORDER = Stats.getStatsRecorder();

      /**
       * Register a view for every measure.
       *
       * <p>If this is not called, e.g. during unit tests, recorded values will not be exported.
       */
      private static void setupOpenCensus() {
        ViewManager viewManager = Stats.getViewManager();
        for (MeasureLong measure : ImmutableList.of(COERCED_TO_INT, NOT_COERCED_TO_INT,
            NOT_COERCED_TO_BOOL)) {
          viewManager.registerView(View.create(Name.create(measure.getName()),
              measure.getDescription(), measure, COUNT_AGGREGATION, ImmutableList.of()));
        }
      }

      /** measure rate of CoercedToInt. */
      @Override
      protected void incrementCoercedToInt() {
        STATS_RECORDER.newMeasureMap().put(COERCED_TO_INT, 1).record();
      }

      /** measure rate of NotCoercedToInt. */
      @Override
      protected void incrementNotCoercedToInt() {
        STATS_RECORDER.newMeasureMap().put(NOT_COERCED_TO_INT, 1).record();
      }

      /** measure rate of NotCoercedToBool. */
      @Override
      protected void incrementNotCoercedToBool() {
        STATS_RECORDER.newMeasureMap().put(NOT_COERCED_TO_BOOL, 1).record();
      }

      /** measure rate of InvalidHistogramType. */
      @Override
      protected void incrementInvalidHistogramType() {
        STATS_RECORDER.newMeasureMap().put(INVALID_HISTOGRAM_TYPE, 1).record();
      }

      /** measure rate of InvalidHistogramSum. */
      @Override
      protected void incrementInvalidHistogramSum() {
        STATS_RECORDER.newMeasureMap().put(INVALID_HISTOGRAM_SUM, 1).record();
      }

      /** measure rate of InvalidHistogramUseCounter. */
      @Override
      protected void incrementInvalidHistogramUseCounter() {
        STATS_RECORDER.newMeasureMap().put(INVALID_HISTOGRAM_USE_COUNTER, 1).record();
      }

      /** measure rate of InvalidHistogramRange. */
      @Override
      protected void incrementInvalidHistogramRange() {
        STATS_RECORDER.newMeasureMap().put(INVALID_HISTOGRAM_RANGE, 1).record();
      }

    }

    public static Payload of(List<String> strictSchemaDocTypes, String schemasLocation,
        IOFunction<String, InputStream> open) {
      return new Payload(strictSchemaDocTypes, schemasLocation, open);
    }

    private final Cache<String, String> normalizedNameCache;
    private final BigQuerySchemaStore schemaStore;
    private final Predicate<Map<String, String>> strictSchema;

    /**
     * Transform attributes and data into {@link ObjectNode} in live table format.
     */
    protected Payload(List<String> strictSchemaDocTypes, String schemasLocation,
        IOFunction<String, InputStream> open) {
      normalizedNameCache = CacheBuilder.newBuilder().maximumSize(50_000).build();
      if (strictSchemaDocTypes == null) {
        strictSchema = attributes -> false;
      } else {
        final HashSet<String> strictSchemaDocTypeSet = new HashSet<>(strictSchemaDocTypes);
        strictSchema = attributes -> {
          String namespace = attributes.getOrDefault(Attribute.DOCUMENT_NAMESPACE, "");
          String docType = attributes.getOrDefault(Attribute.DOCUMENT_TYPE, "");
          return strictSchemaDocTypeSet.contains(namespace + "/" + docType);
        };
      }
      schemaStore = BigQuerySchemaStore.of(schemasLocation, open);
    }

    private Payload(Cache<String, String> normalizedNameCache,
        Predicate<Map<String, String>> strictSchema, BigQuerySchemaStore schemaStore) {
      this.normalizedNameCache = normalizedNameCache;
      this.strictSchema = strictSchema;
      this.schemaStore = schemaStore;
    }

    public WithOpenCensusMetrics withOpenCensusMetrics() {
      return new WithOpenCensusMetrics(normalizedNameCache, strictSchema, schemaStore);
    }

    /** measure rate of CoercedToInt. */
    protected void incrementCoercedToInt() {
    }

    /** measure rate of NotCoercedToInt. */
    protected void incrementNotCoercedToInt() {
    }

    /** measure rate of NotCoercedToBool. */
    protected void incrementNotCoercedToBool() {
    }

    /** measure rate of InvalidHistogramType. */
    protected void incrementInvalidHistogramType() {
    }

    /** measure rate of InvalidHistogramSum. */
    protected void incrementInvalidHistogramSum() {
    }

    /** measure rate of InvalidHistogramUseCounter. */
    protected void incrementInvalidHistogramUseCounter() {
    }

    /** measure rate of InvalidHistogramRange. */
    protected void incrementInvalidHistogramRange() {
    }

    /**
     * Turn message data into an {@link ObjectNode}.
     *
     * <p>{@code data} must not be compressed.
     *
     * <p>We also perform some manipulation of the parsed JSON to match details of our table schemas
     * in BigQuery.
     *
     * <p>If {@code schemasLocation} wasn't provided then {@link TableId} is used to get schemas
     * directly from BigQuery.
     */
    @Override
    public ObjectNode apply(TableId tableId, Map<String, String> attributes, byte[] data) {
      final Schema schema = schemaStore.getSchema(tableId, attributes);

      final ObjectNode contents;
      try {
        contents = Json.readObjectNode(data);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      // Strip metadata so that it's not subject to transformation.
      final JsonNode metadata = contents.remove(AddMetadata.METADATA);

      // Make BQ-specific transformations to the payload structure.
      final ObjectNode additionalProperties = strictSchema.test(attributes) ? null
          : Json.createObjectNode();
      transformForBqSchema(contents, schema.getFields(), additionalProperties);

      if (metadata != null) {
        contents.set(AddMetadata.METADATA, metadata);
      }
      if (additionalProperties != null) {
        contents.put(FieldName.ADDITIONAL_PROPERTIES, Json.asString(additionalProperties));
      }
      return contents;
    }

    /**
     * Recursively descend into the fields of the passed map and compare to the passed BQ schema,
     * while modifying the structure to accommodate map types, nested arrays, etc.
     *
     * @param parent the map object to inspect and transform
     * @param bqFields the list of expected BQ fields inside this object
     * @param additionalProperties a map for storing fields absent in the BQ schema; if null, this
     *                             is "strict schema" mode and additional properties will be dropped
     */
    @VisibleForTesting
    void transformForBqSchema(ObjectNode parent, List<Field> bqFields,
        ObjectNode additionalProperties) {
      final Map<String, Field> bqFieldMap = bqFields.stream()
          .collect(Collectors.toMap(Field::getName, Function.identity()));

      for (String jsonFieldName : Lists.newArrayList(parent.fieldNames())) {
        final JsonNode value = parent.get(jsonFieldName);

        final String bqFieldName;
        if (bqFieldMap.containsKey(jsonFieldName)) {
          // The JSON field name already matches a BQ field.
          bqFieldName = jsonFieldName;
        } else {
          // Remove the json field from the parent because it does not match the BQ field name.
          parent.remove(jsonFieldName);

          // Try cleaning the name to match our BQ conventions.
          bqFieldName = getAndCacheBqName(jsonFieldName);
        }

        // If the field name matches a BQ field name, we process it and add it to the parent,
        // otherwise we add it to additionalProperties without renaming.
        if (bqFieldMap.containsKey(bqFieldName)) {
          processField(jsonFieldName, bqFieldMap.get(bqFieldName), value, parent,
              additionalProperties);
        } else if (additionalProperties != null) {
          additionalProperties.set(jsonFieldName, value);
        }
      }
    }

    /**
     * Return true if this field is a repeated struct of key and maybe value, indicating a JSON map.
     */
    private static boolean isMapType(Field field) {
      return field.getType() == LegacySQLTypeName.RECORD && field.getMode() == Field.Mode.REPEATED
          && ((field.getSubFields().size() == 2 //
              && field.getSubFields().get(0).getName().equals("key") //
              && field.getSubFields().get(1).getName().equals("value"))
              || (field.getSubFields().size() == 1
                  && field.getSubFields().get(0).getName().equals("key")));
    }

    /**
     * Return true if this field is a nested list.
     */
    private static boolean isNestedListType(Field field, JsonNode value) {
      return field.getType() == LegacySQLTypeName.RECORD && field.getMode() == Field.Mode.REPEATED
          && field.getSubFields().size() == 1 //
          && field.getSubFields().get(0).getName().equals("list") //
          && field.getSubFields().get(0).getMode() == Field.Mode.REPEATED //
          && value.isArray();
    }

    private void processField(String jsonFieldName, Field field, JsonNode value, ObjectNode parent,
        ObjectNode additionalProperties) {
      final String bqFieldName = field.getName();

      // null is valid for any type except an element of a list
      if (value.isNull()) {
        updateParent(parent, jsonFieldName, bqFieldName, value);

        // A record of key and value indicates we need to transformForBqSchema a map to an array.
      } else if (isMapType(field)) {
        expandMapType(jsonFieldName, (ObjectNode) value, field, parent, additionalProperties);

        // A record with a single "list" field and a list value should be expanded appropriately.
      } else if (isNestedListType(field, value)) {
        expandNestedListType(jsonFieldName, (ArrayNode) value, field, parent, additionalProperties);

        // We need to recursively call transformForBqSchema on any normal record type.
      } else if (field.getType() == LegacySQLTypeName.RECORD
          && field.getMode() != Field.Mode.REPEATED) {

        // An array signifies a fixed length tuple which should be given anonymous field names.
        if (value.isArray()) {
          updateParent(parent, jsonFieldName, bqFieldName, processTupleField(jsonFieldName,
              field.getSubFields(), (ArrayNode) value, additionalProperties));
        } else {
          // Only transform value if it is not null
          if (value.isObject()) {
            final ObjectNode props = (additionalProperties == null) ? null
                : Json.createObjectNode();
            transformForBqSchema((ObjectNode) value, field.getSubFields(), props);
            if (!Json.isNullOrEmpty(props)) {
              additionalProperties.set(jsonFieldName, props);
            }
          }
          updateParent(parent, jsonFieldName, bqFieldName, value);
        }

        // Likewise, we need to recursively call transformForBqSchema on repeated record types.
      } else if (field.getType() == LegacySQLTypeName.RECORD
          && field.getMode() == Field.Mode.REPEATED) {
        ArrayNode repeatedAdditionalProperties = Json.createArrayNode();
        if (Streams.stream(value).allMatch(JsonNode::isArray)) {
          // Tuples cannot be transformed in place, instead each element of the parent
          // array will need to reference a new transformed object.
          ArrayNode records = (ArrayNode) value;
          for (int i = 0; i < records.size(); i++) {
            final ObjectNode props = additionalProperties == null ? null : Json.createObjectNode();
            records.set(i, processTupleField(jsonFieldName, field.getSubFields(),
                (ArrayNode) records.get(i), props));
            if (!Json.isNullOrEmpty(props)) {
              repeatedAdditionalProperties.add(props.get(jsonFieldName));
            } else {
              repeatedAdditionalProperties.addObject();
            }
          }
        } else {
          ArrayNode filteredValue = Json.createArrayNode();
          for (JsonNode record : value) {
            final ObjectNode props = additionalProperties == null ? null : Json.createObjectNode();
            if (record.isObject()) {
              filteredValue.add(record);
              transformForBqSchema((ObjectNode) record, field.getSubFields(), props);
              if (!Json.isNullOrEmpty(props)) {
                repeatedAdditionalProperties.add(props);
              } else {
                repeatedAdditionalProperties.addObject();
              }
            } else {
              // BigQuery only allows objects in this array, so we insert an empty object instead.
              filteredValue.addObject();
              repeatedAdditionalProperties.add(record);
            }
          }
          value = filteredValue;
        }
        if (!Streams.stream(repeatedAdditionalProperties).allMatch(EMPTY_OBJECT::equals)) {
          additionalProperties.set(jsonFieldName, repeatedAdditionalProperties);
        }
        updateParent(parent, jsonFieldName, bqFieldName, value);

        // If we've made it here, we have a basic type or a list of basic types.
      } else {
        final Optional<JsonNode> coerced = coerceToBqType(value, field);
        // use coerced.orElse(null) to remove the field via updateParent if necessary
        updateParent(parent, jsonFieldName, bqFieldName, coerced.orElse(null));
        // If coerced is not present that means the actual type didn't match expected and we don't
        // define a coercion. We put the value to additional_properties instead.
        if (!coerced.isPresent() && additionalProperties != null) {
          additionalProperties.set(jsonFieldName, value);
        }
      }
    }

    /**
     * A shim into transformForBqSchema for translating tuples into objects and to
     * reconstruct additional properties.
     * @param jsonFieldName The name of the original JSON field.
     * @param fields A list of types for each element of the tuple.
     * @param tuple A list of objects that are mapped into a record.
     * @param additionalProperties A mutable map of elements that aren't captured in the schema.
     * @return A record object with anonymous struct naming.
     */
    private ObjectNode processTupleField(String jsonFieldName, List<Field> fields, ArrayNode tuple,
        ObjectNode additionalProperties) {
      final ObjectNode m = Json.createObjectNode();
      for (int i = 0; i < tuple.size(); i++) {
        m.set(String.format("f%d_", i), tuple.get(i));
      }
      final ObjectNode props = additionalProperties == null ? null : Json.createObjectNode();
      transformForBqSchema(m, fields, props);
      if (!Json.isNullOrEmpty(props)) {
        final ArrayNode tupleAdditionalProperties = Json.createArrayNode();
        for (int i = 0; i < tuple.size(); i++) {
          tupleAdditionalProperties.addNull();
        }
        props.fields().forEachRemaining(e -> {
          int index = Integer.parseInt(e.getKey().substring(1, e.getKey().length() - 1));
          tupleAdditionalProperties.set(index, e.getValue());
        });
        additionalProperties.set(jsonFieldName, tupleAdditionalProperties);
      }
      return m;
    }

    /**
     * Recursively descend into a map type field, expanding to the key/value struct required in
     * BigQuery schemas.
     */
    private void expandMapType(String jsonFieldName, ObjectNode value, Field field,
        ObjectNode parent, ObjectNode additionalProperties) {
      final ObjectNode props = additionalProperties == null ? null : Json.createObjectNode();
      final Optional<Field> valueFieldOption;
      if (field.getSubFields().size() == 2) {
        valueFieldOption = Optional.of(field.getSubFields().get(1));
      } else {
        valueFieldOption = Optional.empty();
        if (props != null) {
          props.setAll(value);
        }
      }

      final ArrayNode unmapped = Json.createArrayNode();
      value.fields().forEachRemaining(e -> {
        ObjectNode kv = Json.createObjectNode();
        valueFieldOption
            .ifPresent(valueField -> processField(e.getKey(), valueField, e.getValue(), kv, props));
        // add key after processField so it can't be dropped due to e.getKey() matching
        // FieldName.KEY when e.getValue() is null or empty or can't be coerced
        unmapped.add(kv.put(FieldName.KEY, e.getKey()));
      });
      if (!Json.isNullOrEmpty(props)) {
        additionalProperties.set(jsonFieldName, props);
      }
      parent.set(field.getName(), unmapped);
    }

    /**
     * Expand nested lists into an object with a list item to support unnested in BigQuery.
     */
    private void expandNestedListType(String jsonFieldName, ArrayNode value, Field field,
        ObjectNode parent, ObjectNode additionalProperties) {
      Field subField = field.getSubFields().get(0);
      ArrayNode repeatedAdditionalProperties = Json.createArrayNode();
      ArrayNode nestedList = Json.createArrayNode().addAll(Streams.stream(value).map(item -> {
        ObjectNode props = additionalProperties == null ? null : Json.createObjectNode();
        ObjectNode map = Json.createObjectNode();
        processField(FieldName.LIST, subField, item, map, props);
        if (!Json.isNullOrEmpty(props)) {
          repeatedAdditionalProperties.add(props.get(FieldName.LIST));
        } else {
          repeatedAdditionalProperties.addNull();
        }
        return map;
      }).collect(Collectors.toList()));

      parent.set(field.getName(), nestedList);

      if (!Streams.stream(repeatedAdditionalProperties).allMatch(JsonNode::isNull)) {
        additionalProperties.set(jsonFieldName, repeatedAdditionalProperties);
      }
    }

    /**
     * This method gives us a chance to perform some additional type coercions in case the BigQuery
     * field type is different from the source data type. This should rarely happen, since only
     * validated payloads get through to this BQ sink path, but there are sets of probes with
     * heterogeneous types that appear as explicit fields in BQ, but are treated as loosely typed
     * maps at the validation phase; we need to catch these or they can cause the entire pipeline
     * to stall.
     *
     * <p>Returning {@link Optional#empty} here indicates that no coercion is defined and that the
     * field should be put to {@code additional_properties}.
     */
    private Optional<JsonNode> coerceToBqType(JsonNode o, Field field) {
      if (field.getMode() == Field.Mode.REPEATED) {
        if (o.isArray()) {
          // We have not yet observed a case where an array type contains values that cannot be
          // coerced to appropriate values, but if it does this will throw NoSuchElementException
          // and prevent the message from being delivered to BigQuery in a form that could lead to
          // data being missed in additional_properties.
          return Optional.of(Json.createArrayNode()
              .addAll(Streams.stream(o).map(v -> coerceSingleValueToBqType(v, field))
                  .map(Optional::get).collect(Collectors.toList())));
        } else {
          return Optional.empty();
        }
      } else {
        return coerceSingleValueToBqType(o, field);
      }
    }

    private Optional<JsonNode> coerceSingleValueToBqType(JsonNode o, Field field) {
      if (field.getType() == LegacySQLTypeName.STRING) {
        if (o.isTextual()) {
          return Optional.of(o);
        } else if (o.isObject() && o.has("histogram_type")) {
          return Optional.of(compactHistogramEncoding(o, field.getName()));
        } else {
          // If not already a string, we JSON-ify the value.
          // We have many fields that we expect to be coerced to string (histograms, userPrefs,
          // etc.)
          // so no point in maintaining a counter here as it will quickly reach many billions.
          return Optional.of(TextNode.valueOf(Json.asString(o)));
        }
        // Our BigQuery schemas use Standard SQL type names, but the BQ API expects legacy SQL
        // type names, so we end up with technically invalid types of INT64 that we need to
        // check for.
      } else if (field.getType() == LegacySQLTypeName.INTEGER
          || StandardSQLTypeName.INT64.name().equals(field.getType().name())) {
        if (o.isInt() || o.isLong()) {
          return Optional.of(o);
        } else if (o.isBoolean()) {
          incrementCoercedToInt();
          // We assume that false is equivalent to zero and true to 1.
          return Optional.of(IntNode.valueOf(o.asBoolean() ? 1 : 0));
        } else {
          incrementNotCoercedToInt();
          return Optional.empty();
        }
        // Our BigQuery schemas use Standard SQL type names, but the BQ API expects legacy SQL
        // type names, so we may end up with technically invalid types of BOOL that we need to
        // check for.
      } else if (field.getType() == LegacySQLTypeName.BOOLEAN
          || StandardSQLTypeName.BOOL.name().equals(field.getType().name())) {
        if (o.isBoolean()) {
          return Optional.of(o);
        } else {
          incrementNotCoercedToBool();
          return Optional.empty();
        }
      } else {
        return Optional.of(o);
      }
    }

    private static void updateParent(ObjectNode parent, String jsonFieldName, String bqFieldName,
        JsonNode value) {
      if (Json.isNullOrEmpty(value)) {
        parent.remove(jsonFieldName);
      } else {
        parent.set(bqFieldName, value);
      }
    }

    private String getAndCacheBqName(String name) {
      try {
        return normalizedNameCache.get(name, () -> convertNameForBq(name));
      } catch (ExecutionException e) {
        throw new UncheckedExecutionException(e.getCause());
      }
    }

    /**
     * Encode the given JSON object as a compact string.
     *
     * <p>We maintain a BigQuery persistent UDF that can parse a variety of formats, so any format
     * used here must first be supported in:
     * https://github.com/mozilla/bigquery-etl/blob/master/mozfun/hist/extract/udf.sql
     *
     * <p>Schema validation should generally ensure that all histograms are well-formed, but we
     * perform some light sanity checking on the values and fall back to encoding the histogram
     * as a JSON string if anything seems off.
     */
    private TextNode compactHistogramEncoding(JsonNode o, String fieldName) {
      final int histogramType = o.path("histogram_type").asInt(-1);
      if (histogramType < 0 || histogramType > 5) {
        incrementInvalidHistogramType();
        return jsonHistogramEncoding(o);
      }
      final long sum = o.path("sum").asLong(-1);
      if (sum < 0) {
        incrementInvalidHistogramSum();
        return jsonHistogramEncoding(o);
      }
      final JsonNode values = o.path("values");
      if (histogramType == 2 && fieldName.startsWith("use_counter2")) {
        // Histograms named as "use_counter" are reported as type 2 (boolean), but only ever have
        // a non-zero value in the "1" (true) bucket (and the "sum" field should match this count).
        // They can be encoded as a textual representation of that single number without any loss
        // of information, and since use counters make up the majority of histogram data volume,
        // this optimization case is the most important one.
        // See https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/collection/use-counters.html
        if (sum == values.path("1").asLong(0)) {
          return TextNode.valueOf(Long.toString(sum));
        } else {
          incrementInvalidHistogramUseCounter();
          return jsonHistogramEncoding(o);
        }
      }
      return jsonHistogramEncoding(o);
      // TODO: Uncomment the following section and related test cases once we have transitioned
      // analysis use cases to tolerate the additional encodings.
      /*
      else if (histogramType == 4) {
        return TextNode.valueOf(Long.toString(sum));
      } else if (histogramType == 2) {
        // Type 2 are "boolean" histograms where bucket "0" is a count of false values and
        // bucket "1" is a count of true values.
        return TextNode.valueOf(
            String.format("%d,%d", values.path("0").longValue(), values.path("1").longValue()));
      } else {
        final int bucketCount = o.path("bucket_count").asInt(-1);
        final long rangeLo = o.path("range").path(0).asLong(-1);
        final long rangeHi = o.path("range").path(1).asLong(-1);
        final String valString = Json.asString(values) //
            .replace("{", "") //
            .replace("}", "") //
            .replace("\"", "");
        if (bucketCount <= 0 || rangeLo < 0 || rangeHi < 0) {
          incrementInvalidHistogramRange();
          return jsonHistogramEncoding(o);
        }
        return TextNode.valueOf(String.format("%d;%d;%d;%d,%d;%s", //
            bucketCount, histogramType, sum, rangeLo, rangeHi, valString));
      }
      */
    }

    private static TextNode jsonHistogramEncoding(JsonNode o) {
      return TextNode.valueOf(Json.asString(o));
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
}
