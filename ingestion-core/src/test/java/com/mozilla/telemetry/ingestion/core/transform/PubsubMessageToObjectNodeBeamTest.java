package com.mozilla.telemetry.ingestion.core.transform;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.Ignore;
import org.junit.Test;

public class PubsubMessageToObjectNodeBeamTest {

  private static final Field MAP_FIELD = Field //
      .newBuilder("map_field", LegacySQLTypeName.RECORD, //
          Field.of("key", LegacySQLTypeName.STRING), //
          Field.of("value", LegacySQLTypeName.INTEGER)) //
      .setMode(Mode.REPEATED).build();

  private static final Field MAP_FIELD_WITHOUT_VALUE = Field //
      .newBuilder("map_field", LegacySQLTypeName.RECORD, //
          Field.of("key", LegacySQLTypeName.STRING)) //
      .setMode(Mode.REPEATED).build();

  private static final PubsubMessageToObjectNode.Payload TRANSFORM = //
      PubsubMessageToObjectNode.Payload.of(null, null, null);

  @Test
  public void testConvertFieldNameForBq() {
    assertEquals("snake_case", PubsubMessageToObjectNode.Payload.convertNameForBq("snake_case"));
    assertEquals("camel_case", PubsubMessageToObjectNode.Payload.convertNameForBq("camelCase"));
    assertEquals("ram", PubsubMessageToObjectNode.Payload.convertNameForBq("RAM"));
    assertEquals("active_gm_plugins",
        PubsubMessageToObjectNode.Payload.convertNameForBq("activeGMPlugins"));
    assertEquals("d2d_enabled", PubsubMessageToObjectNode.Payload.convertNameForBq("D2DEnabled"));
    assertEquals("gpu_active", PubsubMessageToObjectNode.Payload.convertNameForBq("GPUActive"));
    assertEquals("product_id", PubsubMessageToObjectNode.Payload.convertNameForBq("ProductID"));
    assertEquals("l3cache_kb", PubsubMessageToObjectNode.Payload.convertNameForBq("l3cacheKB"));
    assertEquals("_64bit", PubsubMessageToObjectNode.Payload.convertNameForBq("64bit"));
    assertEquals("hi_fi", PubsubMessageToObjectNode.Payload.convertNameForBq("hi-fi"));
    assertEquals("untrusted_modules",
        PubsubMessageToObjectNode.Payload.convertNameForBq("untrustedModules"));
    assertEquals("xul_load_duration_ms",
        PubsubMessageToObjectNode.Payload.convertNameForBq("xulLoadDurationMS"));
    assertEquals("a11y_consumers",
        PubsubMessageToObjectNode.Payload.convertNameForBq("A11Y_CONSUMERS"));
  }

  @Test
  public void testCoerceNestedArrayToJsonString() throws Exception {
    ObjectNode parent = Json.createObjectNode().set("events",
        Json.createArrayNode().add(Json.createArrayNode().add("hi").add("there")));
    List<Field> bqFields = ImmutableList
        .of(Field.newBuilder("events", LegacySQLTypeName.STRING).setMode(Mode.REPEATED).build());
    Map<String, Object> expected = ImmutableMap.of("events",
        ImmutableList.of("[\"hi\",\"there\"]"));
    TRANSFORM.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testNullRecord() throws Exception {
    ObjectNode parent = Json.createObjectNode().putNull("record");
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList.of(
        Field.of("record", LegacySQLTypeName.RECORD, Field.of("field", LegacySQLTypeName.STRING)));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(ImmutableMap.of(), Json.asMap(parent));
    assertEquals(ImmutableMap.of(), Json.asMap(additionalProperties));
  }

  @Test
  public void testCoerceEmptyObjectToJsonString() throws Exception {
    ObjectNode parent = Json.createObjectNode().set("payload", Json.createObjectNode());
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.STRING));
    Map<String, Object> expected = ImmutableMap.of("payload", "{}");
    TRANSFORM.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testCoerceIntToString() throws Exception {
    ObjectNode parent = Json.createObjectNode().put("payload", 3);
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.STRING));
    Map<String, Object> expected = ImmutableMap.of("payload", "3");
    TRANSFORM.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testCoerceMapValueToString() throws Exception {
    String mainPing = "{\"payload\":{\"processes\":{\"parent\":{\"scalars\":"
        + "{\"timestamps.first_paint\":5405}}}}}";
    ObjectNode parent = Json.readObjectNode(mainPing);
    ObjectNode additionalProperties = Json.createObjectNode();
    parent.put("64bit", true);
    parent.put("hi-fi", true);
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.RECORD,
        Field.of("processes", LegacySQLTypeName.RECORD,
            Field.of("parent", LegacySQLTypeName.RECORD,
                Field.newBuilder("scalars", LegacySQLTypeName.RECORD, //
                    Field.of("key", LegacySQLTypeName.STRING), //
                    Field.of("value", LegacySQLTypeName.STRING)) //
                    .setMode(Mode.REPEATED).build()))));
    Map<String, Object> expected = ImmutableMap.of("payload",
        ImmutableMap.of("processes", ImmutableMap.of("parent", ImmutableMap.of("scalars",
            ImmutableList.of(ImmutableMap.of("key", "timestamps.first_paint", "value", "5405"))))));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
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
    Map<String, Object> expected = ImmutableMap.of("payload",
        ImmutableMap.of("processes", ImmutableMap.of("parent", ImmutableMap.of("scalars",
            ImmutableMap.of("contentblocking_category", 0, "contentblocking_exceptions", 0)))));
    Map<String, Object> expectedAdditionalProperties = ImmutableMap.of("payload",
        ImmutableMap.of("processes", ImmutableMap.of("parent", ImmutableMap.of("scalars",
            ImmutableMap.of("contentblocking.other", "should_be_int")))));
    ObjectNode parent = Json.readObjectNode(mainPing);
    ObjectNode additionalProperties = Json.createObjectNode();
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
    assertEquals(expected, Json.asMap(parent));
    assertEquals(expectedAdditionalProperties, Json.asMap(additionalProperties));
  }

  @Test
  public void testCoerceUseCounterHistogramToString() throws Exception {
    String mainPing = "{\"payload\":{\"histograms\":{\"use_counter2_css_property_all_page\":"
        + "{\"bucket_count\":3,\"histogram_type\":2,\"sum\":5,\"range\":[1,2]"
        + ",\"values\":{\"0\":0,\"1\":5,\"2\":0}}}}}";
    ObjectNode parent = Json.readObjectNode(mainPing);
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.RECORD,
        Field.of("histograms", LegacySQLTypeName.RECORD,
            Field.of("use_counter2_css_property_all_page", LegacySQLTypeName.STRING))));
    Map<String, Object> expected = ImmutableMap.of("payload",
        ImmutableMap.of("histograms", ImmutableMap.of("use_counter2_css_property_all_page", "5")));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testCoerceEmptyUseCounterHistogramToString() throws Exception {
    String mainPing = "{\"payload\":{\"histograms\":{\"use_counter2_css_property_all_page\":"
        + "{\"bucket_count\":3,\"histogram_type\":2,\"sum\":0,\"range\":[1,2]"
        + ",\"values\":{}}}}}";
    ObjectNode parent = Json.readObjectNode(mainPing);
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.RECORD,
        Field.of("histograms", LegacySQLTypeName.RECORD,
            Field.of("use_counter2_css_property_all_page", LegacySQLTypeName.STRING))));
    Map<String, Object> expected = ImmutableMap.of("payload",
        ImmutableMap.of("histograms", ImmutableMap.of("use_counter2_css_property_all_page", "0")));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Ignore("Waiting for full compact histogram encoding implementation; see bug 1646825")
  @Test
  public void testCoerceType2HistogramToString() throws Exception {
    String mainPing = "{\"payload\":{\"histograms\":{\"some_histo\":"
        + "{\"bucket_count\":3,\"histogram_type\":2,\"sum\":1,\"range\":[1,2]"
        + ",\"values\":{\"0\":0,\"1\":1,\"2\":0}}}}}";

    ObjectNode parent = Json.readObjectNode(mainPing);
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList
        .of(Field.of("payload", LegacySQLTypeName.RECORD, Field.of("histograms",
            LegacySQLTypeName.RECORD, Field.of("some_histo", LegacySQLTypeName.STRING))));
    Map<String, Object> expected = ImmutableMap.of("payload",
        ImmutableMap.of("histograms", ImmutableMap.of("some_histo", "0,1")));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Ignore("Waiting for full compact histogram encoding implementation; see bug 1646825")
  @Test
  public void testCoerceType4HistogramToString() throws Exception {
    String mainPing = "{\"payload\":{\"histograms\":{\"some_histo\":"
        + "{\"bucket_count\":3,\"histogram_type\":4,\"sum\":3,\"range\":[1,2]"
        + ",\"values\":{\"0\":0,\"1\":3,\"2\":0}}}}}";
    ObjectNode parent = Json.readObjectNode(mainPing);
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList
        .of(Field.of("payload", LegacySQLTypeName.RECORD, Field.of("histograms",
            LegacySQLTypeName.RECORD, Field.of("some_histo", LegacySQLTypeName.STRING))));
    Map<String, Object> expected = ImmutableMap.of("payload",
        ImmutableMap.of("histograms", ImmutableMap.of("some_histo", "3")));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Ignore("Waiting for full compact histogram encoding implementation; see bug 1646825")
  @Test
  public void testCoerceType1HistogramToString() throws Exception {
    String mainPing = "{\"payload\":{\"histograms\":{\"some_histo\":"
        + "{\"bucket_count\":10,\"histogram_type\":1,\"sum\":2628,\"range\":[1,100]"
        + ",\"values\":{\"0\":12434,\"1\":297,\"13\":8}}}}}";
    ObjectNode parent = Json.readObjectNode(mainPing);
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList
        .of(Field.of("payload", LegacySQLTypeName.RECORD, Field.of("histograms",
            LegacySQLTypeName.RECORD, Field.of("some_histo", LegacySQLTypeName.STRING))));
    Map<String, Object> expected = ImmutableMap.of("payload", ImmutableMap.of("histograms",
        ImmutableMap.of("some_histo", "10;1;2628;1,100;0:12434,1:297,13:8")));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testCoerceHistogramFallback() throws Exception {
    // We construct a histogram with invalid negative range to test that we fall back to
    // writing out the JSON explicitly.
    String mainPing = "{\"payload\":{\"histograms\":{\"some_histo\":"
        + "{\"bucket_count\":10,\"histogram_type\":1,\"sum\":2628,\"range\":[-5,100]"
        + ",\"values\":{\"0\":12434,\"1\":297,\"13\":8}}}}}";
    ObjectNode parent = Json.readObjectNode(mainPing);
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList
        .of(Field.of("payload", LegacySQLTypeName.RECORD, Field.of("histograms",
            LegacySQLTypeName.RECORD, Field.of("some_histo", LegacySQLTypeName.STRING))));
    Map<String, Object> expected = ImmutableMap.of("payload",
        ImmutableMap.of("histograms",
            ImmutableMap.of("some_histo",
                "{\"bucket_count\":10,\"histogram_type\":1,\"sum\":2628,\"range\":[-5,100]"
                    + ",\"values\":{\"0\":12434,\"1\":297,\"13\":8}}")));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testUnmap() throws Exception {
    ObjectNode parent = Json.createObjectNode().set("mapField",
        Json.createObjectNode().put("foo", 3).put("bar", 4));
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList.of(MAP_FIELD);
    Map<String, Object> expected = ImmutableMap.of("map_field", ImmutableList
        .of(ImmutableMap.of("key", "foo", "value", 3), ImmutableMap.of("key", "bar", "value", 4)));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testUnmapWithoutValue() throws Exception {
    List<Field> bqFields = ImmutableList.of(MAP_FIELD_WITHOUT_VALUE);
    Map<String, Object> expectedParent = ImmutableMap.of("map_field",
        ImmutableList.of(ImmutableMap.of("key", "foo"), ImmutableMap.of("key", "bar")));
    Map<String, Object> expectedAdditional = ImmutableMap.of("mapField",
        ImmutableMap.of("bar", 4, "foo", 3));
    for (boolean withAdditional : ImmutableList.of(true, false)) {
      ObjectNode additionalProperties = withAdditional ? Json.createObjectNode() : null;
      ObjectNode parent = Json.createObjectNode().set("mapField",
          Json.createObjectNode().put("foo", 3).put("bar", 4));
      TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
      assertEquals(expectedParent, Json.asMap(parent));
      if (withAdditional) {
        assertEquals(expectedAdditional, Json.asMap(additionalProperties));
      }
    }
  }

  @Test
  public void testNestedUnmap() throws Exception {
    ObjectNode parent = Json.createObjectNode().set("outer", Json.createObjectNode()
        .put("otherfield", 3).set("mapField", Json.createObjectNode().put("foo", 3).put("bar", 4)));
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList.of(Field.of("outer", LegacySQLTypeName.RECORD, //
        Field.of("otherfield", LegacySQLTypeName.INTEGER), //
        MAP_FIELD));
    Map<String, Object> expected = ImmutableMap.of("outer",
        ImmutableMap.of("map_field", ImmutableList.of(ImmutableMap.of("key", "foo", "value", 3),
            ImmutableMap.of("key", "bar", "value", 4)), "otherfield", 3));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testAdditionalProperties() throws Exception {
    ObjectNode parent = Json.createObjectNode().set("outer", Json.createObjectNode()
        .put("otherfield", 3).set("mapField", Json.createObjectNode().put("foo", 3).put("bar", 4)));
    parent.put("clientId", "abc123");
    parent.put("otherStrangeIdField", 3);
    List<Field> bqFields = ImmutableList.of(Field.of("client_id", LegacySQLTypeName.STRING), //
        Field.of("outer", LegacySQLTypeName.RECORD, //
            MAP_FIELD));
    Map<String, Object> expected = ImmutableMap.of("client_id", "abc123", "outer",
        ImmutableMap.of("map_field", ImmutableList.of(ImmutableMap.of("key", "foo", "value", 3),
            ImmutableMap.of("key", "bar", "value", 4))));
    Map<String, Object> expectedAdditional = ImmutableMap.of("otherStrangeIdField", 3, "outer",
        ImmutableMap.of("otherfield", 3));
    ObjectNode additionalProperties = Json.createObjectNode();
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
    assertEquals(expectedAdditional, Json.asMap(additionalProperties));
  }

  @Test
  public void testStrictSchema() throws Exception {
    ObjectNode parent = Json.createObjectNode().set("outer", Json.createObjectNode()
        .put("otherfield", 3).set("mapField", Json.createObjectNode().put("foo", 3).put("bar", 4)));
    List<Field> bqFields = ImmutableList.of(Field.of("outer", LegacySQLTypeName.RECORD, //
        MAP_FIELD));
    Map<String, Object> expected = ImmutableMap.of("outer",
        ImmutableMap.of("map_field", ImmutableList.of(ImmutableMap.of("key", "foo", "value", 3),
            ImmutableMap.of("key", "bar", "value", 4))));
    TRANSFORM.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testAdditionalPropertiesStripsEmpty() throws Exception {
    ObjectNode parent = Json.createObjectNode().set("outer", Json.createObjectNode().set(//
        "mapField", Json.createObjectNode().put("foo", 3).put("bar", 4)));
    ObjectNode additionalProperties = Json.createObjectNode();
    List<Field> bqFields = ImmutableList.of(Field.of("outer", LegacySQLTypeName.RECORD, //
        MAP_FIELD));
    Map<String, Object> expectedAdditional = ImmutableMap.of();
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expectedAdditional, Json.asMap(additionalProperties));
  }

  @Test
  public void testPropertyRename() throws Exception {
    ObjectNode parent = Json.createObjectNode();
    ObjectNode additionalProperties = Json.createObjectNode();
    parent.put("64bit", true);
    parent.put("hi-fi", true);
    List<Field> bqFields = ImmutableList.of(Field.of("_64bit", LegacySQLTypeName.BOOLEAN), //
        Field.of("hi_fi", LegacySQLTypeName.BOOLEAN));
    Map<String, Object> expected = ImmutableMap.of("_64bit", true, "hi_fi", true);
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testNestedRepeatedStructs() throws Exception {
    ObjectNode additionalProperties = Json.createObjectNode();
    ObjectNode parent = Json.readObjectNode("{\n" //
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
        + "}\n");
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

    Map<String, Object> expected = ImmutableMap.of("metrics",
        ImmutableList.of(ImmutableMap.of("key", "engine", "value",
            ImmutableMap.of("keyed_histograms",
                ImmutableList.of(ImmutableMap.of("key", "TELEMETRY_TEST_KEYED_HISTOGRAM", "value",
                    ImmutableList.of(
                        ImmutableMap.of("key", "key1", "value", ImmutableMap.of("sum", 1)),
                        ImmutableMap.of("key", "key2", "value", ImmutableMap.of("sum", 0)))))))));
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));

    Map<String, Object> expectedAdditional = ImmutableMap.of("metrics",
        ImmutableMap.of("engine",
            ImmutableMap.of("keyedHistograms",
                ImmutableMap.of("TELEMETRY_TEST_KEYED_HISTOGRAM",
                    ImmutableMap.of("key1", ImmutableMap.of("values", ImmutableMap.of("1", 1)),
                        "key2", ImmutableMap.of("values", ImmutableMap.of()))))));
    assertEquals(expectedAdditional, Json.asMap(additionalProperties));
  }

  @Test
  public void testRawFormat() throws Exception {
    // Use example values for all attributes present in the spec:
    // https://github.com/mozilla/gcp-ingestion/blob/master/docs/edge.md#edge-server-pubsub-message-schema
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
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
        .put("x_pipeline_proxy", "2018-03-12T21:02:18.123456Z").build();
    byte[] data = "test".getBytes(StandardCharsets.UTF_8);
    Map<String, String> expected = ImmutableMap.<String, String>builder().putAll(attributes)
        .put("payload", "dGVzdA==").build();
    Map<String, Object> actual = Json
        .asMap(PubsubMessageToObjectNode.Raw.of().apply(attributes, data));
    assertEquals(expected, actual);
  }

  @Test
  public void testDecodedFormat() throws Exception {
    // Ensure sure we preserve all attributes present in the spec:
    // https://github.com/mozilla/gcp-ingestion/blob/master/docs/decoder.md#decoded-message-metadata-schema
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put("client_id", "5c49ec73-4350-45a0-9c8a-6c8f5aded0da").put("document_version", "4")
        .put("document_id", "6c49ec73-4350-45a0-9c8a-6c8f5aded0cf")
        .put("document_namespace", "telemetry").put("document_type", "main")
        .put("app_name", "Firefox").put("app_version", "58.0.2")
        .put("app_update_channel", "release").put("app_build_id", "20180206200532")
        .put("geo_country", "US").put("geo_subdivision1", "WA").put("geo_subdivision2", "Clark")
        .put("geo_city", "Vancouver").put("isp_db_version", "test db").put("isp_name", "test isp")
        .put("isp_organization", "test org")
        .put("submission_timestamp", "2018-03-12T21:02:18.123456Z")
        .put("date", "Mon, 12 Mar 2018 21:02:18 GMT").put("dnt", "1")
        .put("x_pingsender_version", "1.0").put("x_debug_id", "my_debug_session_1")
        .put("user_agent_browser", "pingsender").put("user_agent_browser_version", "1.0")
        .put("user_agent_os", "Windows").put("user_agent_os_version", "10")
        .put("normalized_app_name", "Firefox").put("normalized_channel", "release")
        .put("normalized_country_code", "US").put("normalized_os", "Windows")
        .put("normalized_os_version", "10").put("sample_id", "42").build();
    byte[] data = "test".getBytes(StandardCharsets.UTF_8);
    Map<String, Object> expected = ImmutableMap.<String, Object>builder()
        .put("client_id", "5c49ec73-4350-45a0-9c8a-6c8f5aded0da")
        .put("document_id", "6c49ec73-4350-45a0-9c8a-6c8f5aded0cf")
        .put("metadata",
            ImmutableMap.<String, Object>builder().put("document_namespace", "telemetry")
                .put("document_version", "4").put("document_type", "main")
                .put("geo",
                    ImmutableMap.<String, String>builder().put("country", "US")
                        .put("subdivision1", "WA").put("subdivision2", "Clark")
                        .put("city", "Vancouver").build())
                .put("isp",
                    ImmutableMap.<String, String>builder().put("db_version", "test db")
                        .put("name", "test isp").put("organization", "test org").build())
                .put("header",
                    ImmutableMap.<String, String>builder()
                        .put("date", "Mon, 12 Mar 2018 21:02:18 GMT").put("dnt", "1")
                        .put("x_pingsender_version", "1.0").put("x_debug_id", "my_debug_session_1")
                        .build())
                .put("uri",
                    ImmutableMap.<String, String>builder().put("app_name", "Firefox")
                        .put("app_version", "58.0.2").put("app_update_channel", "release")
                        .put("app_build_id", "20180206200532").build())
                .put("user_agent",
                    ImmutableMap.<String, String>builder().put("browser", "pingsender")
                        .put("browser_version", "1.0").put("os", "Windows").put("os_version", "10")
                        .build())
                .build())
        .put("submission_timestamp", "2018-03-12T21:02:18.123456Z")
        .put("normalized_app_name", "Firefox").put("normalized_channel", "release")
        .put("normalized_country_code", "US").put("normalized_os", "Windows")
        .put("normalized_os_version", "10").put("sample_id", 42).put("payload", "dGVzdA==").build();
    Map<String, Object> actual = Json
        .asMap(PubsubMessageToObjectNode.Decoded.of().apply(attributes, data));
    assertEquals(expected, actual);
  }

  @Test
  public void testRepeatedRecordAdditionalProperties() throws Exception {
    ObjectNode additionalProperties = Json.createObjectNode();
    ObjectNode parent = Json.readObjectNode("{\n" //
        + "  \"payload\": [\n" //
        + "     {\"a\": 1},\n" //
        + "     {\"a\": 2, \"b\": 22},\n" //
        + "     {\"a\": 3}\n" //
        + "]}\n");
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.of("a", LegacySQLTypeName.INTEGER)) //
        .setMode(Mode.REPEATED).build());
    Map<String, Object> expected = Json.readMap("{\"payload\":[{\"a\":1},{\"a\":2},{\"a\":3}]}");
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));

    Map<String, Object> expectedAdditional = Json.readMap("{\"payload\":[{},{\"b\":22},{}]}");
    assertEquals(expectedAdditional, Json.asMap(additionalProperties));
  }

  @Test
  public void testTupleIntoStruct() throws Exception {
    ObjectNode additionalProperties = Json.createObjectNode();
    ObjectNode parent = Json.readObjectNode("{\n" //
        + "  \"payload\": [\"foo\",26,true],\n" //
        + "  \"additional\": [\"bar\",82,false]\n" //
        + "}\n");
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.of("f0_", LegacySQLTypeName.STRING), //
        Field.of("f1_", LegacySQLTypeName.INTEGER), //
        Field.of("f2_", LegacySQLTypeName.BOOLEAN), //
        Field.newBuilder("f3_", LegacySQLTypeName.STRING).setMode(Mode.NULLABLE).build()) //
        .setMode(Mode.NULLABLE).build());
    Map<String, Object> expected = Json
        .readMap("{\"payload\":{\"f0_\":\"foo\",\"f1_\":26,\"f2_\":true}}");
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));

    Map<String, Object> expectedAdditional = Json.readMap("{\"additional\":[\"bar\",82,false]}");
    assertEquals(expectedAdditional, Json.asMap(additionalProperties));
  }

  @Test
  public void testTupleIntoStructAdditionalProperties() throws Exception {
    ObjectNode additionalProperties = Json.createObjectNode();
    ObjectNode parent = Json.readObjectNode("{\n" //
        + "  \"payload\": [26,{\"a\":83,\"b\":44}]\n" //
        + "}\n");
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.of("f0_", LegacySQLTypeName.INTEGER), //
        Field.of("f1_", LegacySQLTypeName.RECORD, //
            Field.of("a", LegacySQLTypeName.INTEGER))) //
        .setMode(Mode.NULLABLE).build());
    Map<String, Object> expected = Json.readMap("{\"payload\":{\"f0_\":26,\"f1_\":{\"a\":83}}}");
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));

    Map<String, Object> expectedAdditional = Json.readMap("{\"payload\":[null,{\"b\":44}]}");
    assertEquals(expectedAdditional, Json.asMap(additionalProperties));
  }

  @Test
  public void testTupleIntoStructNested() throws Exception {
    ObjectNode additionalProperties = Json.createObjectNode();
    ObjectNode parent = Json.readObjectNode("{\n" //
        + "  \"payload\": [[1],[2],[3]]\n" //
        + "}\n");
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.of("f0_", LegacySQLTypeName.INTEGER)) //
        .setMode(Mode.REPEATED).build());
    Map<String, Object> expected = Json
        .readMap("{\"payload\":[{\"f0_\":1},{\"f0_\":2},{\"f0_\":3}]}");
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testNestedList() throws Exception {
    ObjectNode additionalProperties = Json.createObjectNode();
    ObjectNode parent = Json.readObjectNode("{\n" //
        + "  \"payload\": [[0],[1]]\n" //
        + "}\n");
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.newBuilder("list", LegacySQLTypeName.INTEGER) //
            .setMode(Mode.REPEATED).build() //
    ).setMode(Mode.REPEATED).build()); //
    Map<String, Object> expected = Json.readMap("{\"payload\":[{\"list\":[0]},{\"list\":[1]}]}");
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }

  @Test
  public void testNestedListAdditionalProperties() throws Exception {
    ObjectNode additionalProperties = Json.createObjectNode();
    ObjectNode parent = Json.readObjectNode("{\n" //
        + "  \"payload\": [[{\"a\":1}],[{\"a\":2},{\"a\":3,\"b\":4}]]\n" //
        + "}\n");
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.newBuilder("list", LegacySQLTypeName.RECORD, //
            Field.of("a", LegacySQLTypeName.INTEGER)) //
            .setMode(Mode.REPEATED).build() //
    ).setMode(Mode.REPEATED).build()); //
    Map<String, Object> expected = Json
        .readMap("{\"payload\":[{\"list\":[{\"a\":1}]},{\"list\":[{\"a\":2},{\"a\":3}]}]}");
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));

    Map<String, Object> expectedAdditional = Json.readMap("{\"payload\":[null,[{},{\"b\":4}]]}");
    assertEquals(expectedAdditional, Json.asMap(additionalProperties));
  }

  @Test
  public void testListWithNulls() throws Exception {
    ObjectNode additionalProperties = Json.createObjectNode();
    ObjectNode parent = Json.readObjectNode(
        ("{\"modules\":[{\"base_addr\":\"0x1390000\"},null]}").getBytes(StandardCharsets.UTF_8));
    List<Field> bqFields = ImmutableList.of(Field
        .newBuilder("modules", LegacySQLTypeName.RECORD,
            Field.of("base_addr", LegacySQLTypeName.STRING)) //
        .setMode(Mode.REPEATED).build() //
    ); //
    Map<String, Object> expected = Json.readMap("{\"modules\":[{\"base_addr\":\"0x1390000\"},{}]}");
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));

    Map<String, Object> expectedAdditional = Json.readMap("{\"modules\":[{},null]}");
    assertEquals(expectedAdditional, Json.asMap(additionalProperties));
  }

  @Test
  public void testDoublyNestedList() throws Exception {
    ObjectNode additionalProperties = Json.createObjectNode();
    ObjectNode parent = Json.readObjectNode("{\n" //
        + "  \"payload\": [[[0],[1]],[[2]]]\n" //
        + "}\n");
    List<Field> bqFields = ImmutableList.of(Field.newBuilder("payload", LegacySQLTypeName.RECORD, //
        Field.newBuilder("list", LegacySQLTypeName.RECORD, //
            Field.newBuilder("list", LegacySQLTypeName.INTEGER).setMode(Mode.REPEATED).build()) //
            .setMode(Mode.REPEATED).build() //
    ).setMode(Mode.REPEATED).build()); //
    Map<String, Object> expected = Json.readMap("{\"payload\":[" //
        + "{\"list\":[{\"list\":[0]},{\"list\":[1]}]}," //
        + "{\"list\":[{\"list\":[2]}]}" //
        + "]}");
    TRANSFORM.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asMap(parent));
  }
}
