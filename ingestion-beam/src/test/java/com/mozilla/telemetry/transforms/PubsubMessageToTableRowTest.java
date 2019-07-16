/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;

public class PubsubMessageToTableRowTest extends TestWithDeterministicJson {

  private static final Field MAP_FIELD = Field //
      .newBuilder("map_field", LegacySQLTypeName.RECORD, //
          Field.of("key", LegacySQLTypeName.STRING), //
          Field.of("value", LegacySQLTypeName.INTEGER)) //
      .setMode(Mode.REPEATED).build();

  private static final PubsubMessageToTableRow TRANSFORM = PubsubMessageToTableRow
      .of(StaticValueProvider.of("foo"), null, null, null);

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
    Map<String, Object> parent = Json.readTableRow(mainPing.getBytes());
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
    String expectedAdditional = "{\"other_strange_id_field\":3,\"outer\":{\"other_field\":3}}";
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
        + "}\n").getBytes());
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

    String expectedAdditional = "{\"metrics\":{\"engine\":{\"keyed_histograms\":"
        + "{\"TELEMETRY_TEST_KEYED_HISTOGRAM\":"
        + "{\"key1\":{\"values\":{\"1\":1}},\"key2\":{\"values\":{}}}}}}}";
    assertEquals(expectedAdditional, Json.asString(additionalProperties));
  }

}
