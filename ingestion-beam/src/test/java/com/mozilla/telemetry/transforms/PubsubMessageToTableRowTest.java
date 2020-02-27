package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow.TableRowFormat;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;

public class PubsubMessageToTableRowTest extends TestWithDeterministicJson {

  private static final Field MAP_FIELD = Field //
      .newBuilder("map_field", LegacySQLTypeName.RECORD, //
          Field.of("key", LegacySQLTypeName.STRING), //
          Field.of("value", LegacySQLTypeName.INTEGER)) //
      .setMode(Mode.REPEATED).build();

  private static final Field MAP_FIELD_WITHOUT_VALUE = Field //
      .newBuilder("map_field", LegacySQLTypeName.RECORD, //
          Field.of("key", LegacySQLTypeName.STRING)) //
      .setMode(Mode.REPEATED).build();

  private static final KeyByBigQueryTableDestination KEY_BY = KeyByBigQueryTableDestination.of(
      StaticValueProvider.of("foo"), StaticValueProvider.of(null), StaticValueProvider.of(null));

  private static final PubsubMessageToTableRow TRANSFORM = PubsubMessageToTableRow.of(null, null,
      StaticValueProvider.of(TableRowFormat.payload), KEY_BY);

  @Test
  public void testConvertFieldNameForBq() {
    assertEquals("snake_case", PubsubMessageToTableRow.convertNameForBq("snake_case"));
    assertEquals("camel_case", PubsubMessageToTableRow.convertNameForBq("camelCase"));
    assertEquals("ram", PubsubMessageToTableRow.convertNameForBq("RAM"));
    assertEquals("active_gm_plugins", PubsubMessageToTableRow.convertNameForBq("activeGMPlugins"));
    assertEquals("d2d_enabled", PubsubMessageToTableRow.convertNameForBq("D2DEnabled"));
    assertEquals("gpu_active", PubsubMessageToTableRow.convertNameForBq("GPUActive"));
    assertEquals("product_id", PubsubMessageToTableRow.convertNameForBq("ProductID"));
    assertEquals("l3cache_kb", PubsubMessageToTableRow.convertNameForBq("l3cacheKB"));
    assertEquals("_64bit", PubsubMessageToTableRow.convertNameForBq("64bit"));
    assertEquals("hi_fi", PubsubMessageToTableRow.convertNameForBq("hi-fi"));
    assertEquals("untrusted_modules", PubsubMessageToTableRow.convertNameForBq("untrustedModules"));
    assertEquals("xul_load_duration_ms",
        PubsubMessageToTableRow.convertNameForBq("xulLoadDurationMS"));
    assertEquals("a11y_consumers", PubsubMessageToTableRow.convertNameForBq("A11Y_CONSUMERS"));
  }

  @Test
  public void testCoerceNestedArrayToJsonString() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    parent.put("events", Arrays.asList(Arrays.asList("hi", "there")));
    List<Field> bqFields = ImmutableList
        .of(Field.newBuilder("events", LegacySQLTypeName.STRING).setMode(Mode.REPEATED).build());
    String expected = "{\"events\":[\"[\\\"hi\\\",\\\"there\\\"]\"]}";
    TRANSFORM.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testNullRecord() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    Map<String, Object> additionalProperties = new HashMap<>();
    parent.put("record", null);
    List<Field> bqFields = ImmutableList.of(
        Field.of("record", LegacySQLTypeName.RECORD, Field.of("field", LegacySQLTypeName.STRING)));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals("{\"record\":null}", Json.asString(parent));
    assertEquals("{}", Json.asString(additionalProperties));
  }

  @Test
  public void testCoerceEmptyObjectToJsonString() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    parent.put("payload", new HashMap<>());
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.STRING));
    String expected = "{\"payload\":\"{}\"}";
    TRANSFORM.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testCoerceIntToString() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    parent.put("payload", 3);
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.STRING));
    String expected = "{\"payload\":\"3\"}";
    TRANSFORM.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testCoerceMapValueToString() throws Exception {
    String mainPing = "{\"payload\":{\"processes\":{\"parent\":{\"scalars\":"
        + "{\"timestamps.first_paint\":5405}}}}}";
    Map<String, Object> parent = Json.readTableRow(mainPing.getBytes(StandardCharsets.UTF_8));
    Map<String, Object> additionalProperties = new HashMap<>();
    parent.put("64bit", true);
    parent.put("hi-fi", true);
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.RECORD,
        Field.of("processes", LegacySQLTypeName.RECORD,
            Field.of("parent", LegacySQLTypeName.RECORD,
                Field.newBuilder("scalars", LegacySQLTypeName.RECORD, //
                    Field.of("key", LegacySQLTypeName.STRING), //
                    Field.of("value", LegacySQLTypeName.STRING)) //
                    .setMode(Mode.REPEATED).build()))));
    String expected = "{\"payload\":{\"processes\":{\"parent\":{\"scalars\":"
        + "[{\"key\":\"timestamps.first_paint\",\"value\":\"5405\"}]}}}}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

  /**
   * We have observed clients very occasionally (less than once per month across all clients)
   * sending a boolean or string for an integer probe.
   */
  @Test
  public void testTypeCoercion() throws Exception {
    String mainPing = "{\"payload\":{\"processes\":{\"parent\":{\"scalars\":"
        + "{\"contentblocking.exceptions\":false" //
        + ",\"contentblocking.other\":\"should_be_int\"" //
        + ",\"contentblocking.category\":0}}}}}";
    String expected = "{\"payload\":{\"processes\":{\"parent\":{\"scalars\":"
        + "{\"contentblocking_category\":0,\"contentblocking_exceptions\":0}}}}}";
    String expectedAdditionalProperties = "{\"payload\":{\"processes\":{\"parent\":{\"scalars\":"
        + "{\"contentblocking.other\":\"should_be_int\"}}}}}";
    Map<String, Object> parent = Json.readTableRow(mainPing.getBytes(StandardCharsets.UTF_8));
    Map<String, Object> additionalProperties = new HashMap<>();
    List<Field> bqFields = ImmutableList
        .of(Field
            .of("payload", LegacySQLTypeName.RECORD,
                Field
                    .of("processes", LegacySQLTypeName.RECORD,
                        Field
                            .of("parent", LegacySQLTypeName.RECORD, Field
                                .newBuilder("scalars", LegacySQLTypeName.RECORD, //
                                    Field.of("contentblocking_exceptions",
                                        LegacySQLTypeName.INTEGER),
                                    Field.of("contentblocking_category", LegacySQLTypeName.INTEGER))
                                .build()))));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
    assertEquals(expectedAdditionalProperties, Json.asString(additionalProperties));
  }

  @Test
  public void testUnmap() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    Map<String, Object> additionalProperties = new HashMap<>();
    parent.put("mapField", new HashMap<>(ImmutableMap.of("foo", 3, "bar", 4)));
    List<Field> bqFields = ImmutableList.of(MAP_FIELD);
    String expected = "{\"map_field\":"
        + "[{\"key\":\"bar\",\"value\":4},{\"key\":\"foo\",\"value\":3}]}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testUnmapWithoutValue() throws Exception {
    List<Field> bqFields = ImmutableList.of(MAP_FIELD_WITHOUT_VALUE);
    String expectedParent = "{\"map_field\":[{\"key\":\"bar\"},{\"key\":\"foo\"}]}";
    String expectedAdditional = "{\"mapField\":{\"bar\":4,\"foo\":3}}";
    for (boolean withAdditional : ImmutableList.of(true, false)) {
      Map<String, Object> additionalProperties = withAdditional ? new HashMap<>() : null;
      Map<String, Object> parent = new HashMap<>(
          ImmutableMap.of("mapField", new HashMap<>(ImmutableMap.of("foo", 3, "bar", 4))));
      TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
      assertEquals(expectedParent, Json.asString(parent));
      if (withAdditional) {
        assertEquals(expectedAdditional, Json.asString(additionalProperties));
      }
    }
  }

  @Test
  public void testNestedUnmap() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    Map<String, Object> additionalProperties = new HashMap<>();
    parent.put("outer", new HashMap<>(ImmutableMap.of("otherfield", 3, //
        "mapField", new HashMap<>(ImmutableMap.of("foo", 3, "bar", 4)))));
    List<Field> bqFields = ImmutableList.of(Field.of("outer", LegacySQLTypeName.RECORD, //
        Field.of("otherfield", LegacySQLTypeName.INTEGER), //
        MAP_FIELD));
    String expected = "{\"outer\":{"
        + "\"map_field\":[{\"key\":\"bar\",\"value\":4},{\"key\":\"foo\",\"value\":3}],"
        + "\"otherfield\":3}}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testAdditionalProperties() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    parent.put("outer", new HashMap<>(ImmutableMap.of("otherField", 3, //
        "mapField", new HashMap<>(ImmutableMap.of("foo", 3, "bar", 4)))));
    parent.put("clientId", "abc123");
    parent.put("otherStrangeIdField", 3);
    List<Field> bqFields = ImmutableList.of(Field.of("client_id", LegacySQLTypeName.STRING), //
        Field.of("outer", LegacySQLTypeName.RECORD, //
            MAP_FIELD));
    String expected = "{\"client_id\":\"abc123\",\"outer\":{"
        + "\"map_field\":[{\"key\":\"bar\",\"value\":4},{\"key\":\"foo\",\"value\":3}]}}";
    String expectedAdditional = "{\"otherStrangeIdField\":3,\"outer\":{\"otherField\":3}}";
    Map<String, Object> additionalProperties = new HashMap<>();
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
    assertEquals(expectedAdditional, Json.asString(additionalProperties));
  }

  @Test
  public void testStrictSchema() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    parent.put("outer", new HashMap<>(ImmutableMap.of("otherfield", 3, //
        "mapField", new HashMap<>(ImmutableMap.of("foo", 3, "bar", 4)))));
    List<Field> bqFields = ImmutableList.of(Field.of("outer", LegacySQLTypeName.RECORD, //
        MAP_FIELD));
    String expected = "{\"outer\":{"
        + "\"map_field\":[{\"key\":\"bar\",\"value\":4},{\"key\":\"foo\",\"value\":3}]}}";
    TRANSFORM.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testAdditionalPropertiesStripsEmpty() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    Map<String, Object> additionalProperties = new HashMap<>();
    parent.put("outer", new HashMap<>(ImmutableMap.of(//
        "mapField", new HashMap<>(ImmutableMap.of("foo", 3, "bar", 4)))));
    List<Field> bqFields = ImmutableList.of(Field.of("outer", LegacySQLTypeName.RECORD, //
        MAP_FIELD));
    String expectedAdditional = "{}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expectedAdditional, Json.asString(additionalProperties));
  }

  @Test
  public void testPropertyRename() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    Map<String, Object> additionalProperties = new HashMap<>();
    parent.put("64bit", true);
    parent.put("hi-fi", true);
    List<Field> bqFields = ImmutableList.of(Field.of("_64bit", LegacySQLTypeName.BOOLEAN), //
        Field.of("hi_fi", LegacySQLTypeName.BOOLEAN));
    String expected = "{\"_64bit\":true,\"hi_fi\":true}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testNestedRepeatedStructs() throws Exception {
    Map<String, Object> additionalProperties = new HashMap<>();
    TableRow parent = Json.readTableRow(("{\n" //
        + "  \"metrics\": {\n" //
        + "    \"engine\": {\n" //
        + "      \"keyedHistograms\": {\n" //
        + "        \"TELEMETRY_TEST_KEYED_HISTOGRAM\":{\n" //
        + "          \"key1\": {\n" //
        + "            \"sum\": 1,\n" //
        + "            \"values\": {\"1\": 1}\n" //
        + "          },\n" //
        + "          \"key2\": {\n" //
        + "            \"sum\": 0,\n" //
        + "            \"values\": {}\n" //
        + "          }\n" //
        + "        }\n" //
        + "      }\n" //
        + "    }\n" //
        + "  }\n" //
        + "}\n").getBytes(StandardCharsets.UTF_8));
    List<Field> bqFields = ImmutableList.of(Field
        .newBuilder("metrics", LegacySQLTypeName.RECORD, Field.of("key", LegacySQLTypeName.STRING),
            Field.of("value", LegacySQLTypeName.RECORD,
                Field.newBuilder("keyed_histograms", LegacySQLTypeName.RECORD,
                    Field.of("key", LegacySQLTypeName.STRING),
                    Field
                        .newBuilder("value", LegacySQLTypeName.RECORD,
                            Field.of("key", LegacySQLTypeName.STRING),
                            Field.of("value", LegacySQLTypeName.RECORD,
                                Field.of("sum", LegacySQLTypeName.INTEGER)))
                        .setMode(Mode.REPEATED).build())
                    .setMode(Mode.REPEATED).build()))
        .setMode(Mode.REPEATED).build());
    String expected = "{\"metrics\":[{\"key\":\"engine\",\"value\":{\"keyed_histograms\":"
        + "[{\"key\":\"TELEMETRY_TEST_KEYED_HISTOGRAM\",\"value\":"
        + "[{\"key\":\"key1\",\"value\":{\"sum\":1}}"
        + ",{\"key\":\"key2\",\"value\":{\"sum\":0}}]}]}}]}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));

    String expectedAdditional = "{\"metrics\":{\"engine\":{\"keyedHistograms\":"
        + "{\"TELEMETRY_TEST_KEYED_HISTOGRAM\":"
        + "{\"key1\":{\"values\":{\"1\":1}},\"key2\":{\"values\":{}}}}}}}";
    assertEquals(expectedAdditional, Json.asString(additionalProperties));
  }

  @Test
  public void testRawFormat() throws Exception {
    PubsubMessage message = new PubsubMessage("test".getBytes(StandardCharsets.UTF_8),
        // Use example values for all attributes present in the spec:
        // https://github.com/mozilla/gcp-ingestion/blob/master/docs/edge.md#edge-server-pubsub-message-schema
        ImmutableMap.<String, String>builder()
            .put("submission_timestamp", "2018-03-12T21:02:18.123456Z")
            .put("uri",
                "/submit/telemetry/6c49ec73-4350-45a0-9c8a-6c8f5aded0cf/main/Firefox/58.0.2"
                    + "/release/20180206200532")
            .put("protocol", "HTTP/1.1").put("method", "POST").put("args", "v=4")
            .put("remote_addr", "172.31.32.5").put("content_length", "4722")
            .put("date", "Mon, 12 Mar 2018 21:02:18 GMT").put("dnt", "1")
            .put("host", "incoming.telemetry.mozilla.org").put("user_agent", "pingsender/1.0")
            .put("x_forwarded_for", "10.98.132.74, 103.3.237.12").put("x_pingsender_version", "1.0")
            .put("x_debug_id", "my_debug_session_1")
            .put("x_pipeline_proxy", "2018-03-12T21:02:18.123456Z").build());
    String expected = Json.asString(ImmutableMap.<String, String>builder()
        .putAll(message.getAttributeMap()).put("payload", "dGVzdA==").build());
    String actual = Json.asString(PubsubMessageToTableRow.rawTableRow(message));
    assertEquals(expected, actual);
  }

  @Test
  public void testDecodedFormat() throws Exception {

    PubsubMessage message = new PubsubMessage("test".getBytes(StandardCharsets.UTF_8),
        // Ensure sure we preserve all attributes present in the spec:
        // https://github.com/mozilla/gcp-ingestion/blob/master/docs/decoder.md#decoded-message-metadata-schema
        ImmutableMap.<String, String>builder()
            .put("client_id", "5c49ec73-4350-45a0-9c8a-6c8f5aded0da").put("document_version", "4")
            .put("document_id", "6c49ec73-4350-45a0-9c8a-6c8f5aded0cf")
            .put("document_namespace", "telemetry").put("document_type", "main")
            .put("app_name", "Firefox").put("app_version", "58.0.2")
            .put("app_update_channel", "release").put("app_build_id", "20180206200532")
            .put("geo_country", "US").put("geo_subdivision1", "WA").put("geo_subdivision2", "Clark")
            .put("geo_city", "Vancouver").put("submission_timestamp", "2018-03-12T21:02:18.123456Z")
            .put("date", "Mon, 12 Mar 2018 21:02:18 GMT").put("dnt", "1")
            .put("x_pingsender_version", "1.0").put("x_debug_id", "my_debug_session_1")
            .put("user_agent_browser", "pingsender").put("user_agent_browser_version", "1.0")
            .put("user_agent_os", "Windows").put("user_agent_os_version", "10")
            .put("normalized_app_name", "Firefox").put("normalized_channel", "release")
            .put("normalized_country_code", "US").put("normalized_os", "Windows")
            .put("normalized_os_version", "10").put("sample_id", "42").build());
    String expected = Json.asString(ImmutableMap.<String, Object>builder()
        .put("client_id", "5c49ec73-4350-45a0-9c8a-6c8f5aded0da")
        .put("document_id", "6c49ec73-4350-45a0-9c8a-6c8f5aded0cf")
        .put("metadata", ImmutableMap.<String, Object>builder()
            .put("document_namespace", "telemetry").put("document_version", "4")
            .put("document_type", "main")
            .put("geo",
                ImmutableMap.<String, String>builder().put("country", "US")
                    .put("subdivision1", "WA").put("subdivision2", "Clark").put("city", "Vancouver")
                    .build())
            .put("header",
                ImmutableMap.<String, String>builder().put("date", "Mon, 12 Mar 2018 21:02:18 GMT")
                    .put("dnt", "1").put("x_pingsender_version", "1.0")
                    .put("x_debug_id", "my_debug_session_1").build())
            .put("uri",
                ImmutableMap.<String, String>builder().put("app_name", "Firefox")
                    .put("app_version", "58.0.2").put("app_update_channel", "release")
                    .put("app_build_id", "20180206200532").build())
            .put("user_agent", ImmutableMap.<String, String>builder().put("browser", "pingsender")
                .put("browser_version", "1.0").put("os", "Windows").put("os_version", "10").build())
            .build())
        .put("submission_timestamp", "2018-03-12T21:02:18.123456Z")
        .put("normalized_app_name", "Firefox").put("normalized_channel", "release")
        .put("normalized_country_code", "US").put("normalized_os", "Windows")
        .put("normalized_os_version", "10").put("sample_id", 42).put("payload", "dGVzdA==")
        .build());
    String actual = Json.asString(PubsubMessageToTableRow.decodedTableRow(message));
    assertEquals(expected, actual);
  }

  @Test
  public void testRepeatedRecordAdditionalProperties() throws Exception {
    Map<String, Object> additionalProperties = new HashMap<>();
    TableRow parent = Json.readTableRow(("{\n" //
        + "  \"payload\": [\n" //
        + "     {\"a\": 1},\n" //
        + "     {\"a\": 2, \"b\": 22},\n" //
        + "     {\"a\": 3}\n" //
        + "]}\n").getBytes(StandardCharsets.UTF_8));
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.of("a", LegacySQLTypeName.INTEGER)) //
        .setMode(Mode.REPEATED).build());
    String expected = "{\"payload\":[{\"a\":1},{\"a\":2},{\"a\":3}]}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));

    String expectedAdditional = "{\"payload\":[null,{\"b\":22},null]}";
    assertEquals(expectedAdditional, Json.asString(additionalProperties));
  }

  @Test
  public void testTupleIntoStruct() throws Exception {
    Map<String, Object> additionalProperties = new HashMap<>();
    TableRow parent = Json.readTableRow(("{\n" //
        + "  \"payload\": [\"foo\",26,true],\n" //
        + "  \"additional\": [\"bar\",82,false]\n" //
        + "}\n").getBytes(StandardCharsets.UTF_8));
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.of("f0_", LegacySQLTypeName.STRING), //
        Field.of("f1_", LegacySQLTypeName.INTEGER), //
        Field.of("f2_", LegacySQLTypeName.BOOLEAN), //
        Field.newBuilder("f3_", LegacySQLTypeName.STRING).setMode(Mode.NULLABLE).build()) //
        .setMode(Mode.NULLABLE).build());
    String expected = "{\"payload\":{\"f0_\":\"foo\",\"f1_\":26,\"f2_\":true}}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));

    String expectedAdditional = "{\"additional\":[\"bar\",82,false]}";
    assertEquals(expectedAdditional, Json.asString(additionalProperties));
  }

  @Test
  public void testTupleIntoStructAdditionalProperties() throws Exception {
    Map<String, Object> additionalProperties = new HashMap<>();
    TableRow parent = Json.readTableRow(("{\n" //
        + "  \"payload\": [26,{\"a\":83,\"b\":44}]\n" //
        + "}\n").getBytes(StandardCharsets.UTF_8));
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.of("f0_", LegacySQLTypeName.INTEGER), //
        Field.of("f1_", LegacySQLTypeName.RECORD, //
            Field.of("a", LegacySQLTypeName.INTEGER))) //
        .setMode(Mode.NULLABLE).build());
    String expected = "{\"payload\":{\"f0_\":26,\"f1_\":{\"a\":83}}}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));

    String expectedAdditional = "{\"payload\":[null,{\"b\":44}]}";
    assertEquals(expectedAdditional, Json.asString(additionalProperties));
  }

  @Test
  public void testTupleIntoStructNested() throws Exception {
    Map<String, Object> additionalProperties = new HashMap<>();
    TableRow parent = Json.readTableRow(("{\n" //
        + "  \"payload\": [[1],[2],[3]]\n" //
        + "}\n").getBytes(StandardCharsets.UTF_8));
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.of("f0_", LegacySQLTypeName.INTEGER)) //
        .setMode(Mode.REPEATED).build());
    String expected = "{\"payload\":[{\"f0_\":1},{\"f0_\":2},{\"f0_\":3}]}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testNestedList() throws Exception {
    Map<String, Object> additionalProperties = new HashMap<>();
    TableRow parent = Json.readTableRow(("{\n" //
        + "  \"payload\": [[0],[1]]\n" //
        + "}\n").getBytes(StandardCharsets.UTF_8));
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.newBuilder("list", LegacySQLTypeName.INTEGER) //
            .setMode(Mode.REPEATED).build() //
    ).setMode(Mode.REPEATED).build()); //
    String expected = "{\"payload\":[{\"list\":[0]},{\"list\":[1]}]}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testNestedListAdditionalProperties() throws Exception {
    Map<String, Object> additionalProperties = new HashMap<>();
    TableRow parent = Json.readTableRow(("{\n" //
        + "  \"payload\": [[{\"a\":1}],[{\"a\":2},{\"a\":3,\"b\":4}]]\n" //
        + "}\n").getBytes(StandardCharsets.UTF_8));
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.newBuilder("list", LegacySQLTypeName.RECORD, //
            Field.of("a", LegacySQLTypeName.INTEGER)) //
            .setMode(Mode.REPEATED).build() //
    ).setMode(Mode.REPEATED).build()); //
    String expected = "{\"payload\":[{\"list\":[{\"a\":1}]},{\"list\":[{\"a\":2},{\"a\":3}]}]}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));

    String expectedAdditional = "{\"payload\":[null,[null,{\"b\":4}]]}";
    assertEquals(expectedAdditional, Json.asString(additionalProperties));
  }

  @Test
  public void testDoublyNestedList() throws Exception {
    Map<String, Object> additionalProperties = new HashMap<>();
    TableRow parent = Json.readTableRow(("{\n" //
        + "  \"payload\": [[[0],[1]],[[2]]]\n" //
        + "}\n").getBytes(StandardCharsets.UTF_8));
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.newBuilder("list", LegacySQLTypeName.RECORD, //
            Field.newBuilder("list", LegacySQLTypeName.INTEGER).setMode(Mode.REPEATED).build()) //
            .setMode(Mode.REPEATED).build() //
    ).setMode(Mode.REPEATED).build()); //
    String expected = "{\"payload\":[" //
        + "{\"list\":[{\"list\":[0]},{\"list\":[1]}]}," //
        + "{\"list\":[{\"list\":[2]}]}" //
        + "]}";
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }
}
