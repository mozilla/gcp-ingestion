/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertEquals;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.util.Json;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class PubsubMessageToTableRowTest {

  static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  static final Field MAP_FIELD = Field //
      .newBuilder("mapfield", LegacySQLTypeName.RECORD, //
          Field.of("key", LegacySQLTypeName.STRING), //
          Field.of("value", LegacySQLTypeName.INTEGER)) //
      .setMode(Mode.REPEATED).build();

  @Test
  public void testBuildTableRow() throws Exception {
    String input = "{\"metadata\":{\"somenum\":3,\"submission_timestamp\":\"2018-01-01T12:13:14\"}"
        + ",\"otherkey\":\"hi\"}";
    String expected = "{\"metadata\":{\"somenum\":3}"
        + ",\"otherkey\":\"hi\",\"submission_timestamp\":\"2018-01-01T12:13:14\"}";
    TableRow tableRow = PubsubMessageToTableRow.buildTableRow(input.getBytes());
    tableRow.setFactory(new JacksonFactory());
    assertEquals(expected, tableRow.toString());
  }

  @Test
  public void testCoerceNestedArrayToJsonString() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    parent.put("events", Arrays.asList(Arrays.asList("hi", "there")));
    List<Field> bqFields = ImmutableList
        .of(Field.newBuilder("events", LegacySQLTypeName.STRING).setMode(Mode.REPEATED).build());
    String expected = "{\"events\":[\"[\\\"hi\\\",\\\"there\\\"]\"]}";
    PubsubMessageToTableRow.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testCoerceEmptyObjectToJsonString() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    parent.put("payload", new HashMap<>());
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.STRING));
    String expected = "{\"payload\":\"{}\"}";
    PubsubMessageToTableRow.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testCoerceIntToString() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    parent.put("payload", 3);
    List<Field> bqFields = ImmutableList.of(Field.of("payload", LegacySQLTypeName.STRING));
    String expected = "{\"payload\":\"3\"}";
    PubsubMessageToTableRow.transformForBqSchema(parent, bqFields, null);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testCoerceMapValueToString() throws Exception {
    String mainPing = "{\"payload\":{\"processes\":{\"parent\":{\"scalars\":"
        + "{\"timestamps.first_paint\":5405}}}}}";
    Map<String, Object> parent = JSON_FACTORY.fromString(mainPing, Map.class);
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
    PubsubMessageToTableRow.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testUnmap() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    Map<String, Object> additionalProperties = new HashMap<>();
    parent.put("mapfield", new HashMap<>(ImmutableMap.of("foo", 3, "bar", 4)));
    List<Field> bqFields = ImmutableList.of(MAP_FIELD);
    String expected = "{\"mapfield\":"
        + "[{\"key\":\"bar\",\"value\":4},{\"key\":\"foo\",\"value\":3}]}";
    PubsubMessageToTableRow.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testNestedUnmap() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    Map<String, Object> additionalProperties = new HashMap<>();
    parent.put("outer", new HashMap<>(ImmutableMap.of("otherfield", 3, //
        "mapfield", new HashMap<>(ImmutableMap.of("foo", 3, "bar", 4)))));
    List<Field> bqFields = ImmutableList.of(Field.of("outer", LegacySQLTypeName.RECORD, //
        Field.of("otherfield", LegacySQLTypeName.INTEGER), //
        MAP_FIELD));
    String expected = "{\"outer\":{\"otherfield\":3"
        + ",\"mapfield\":[{\"key\":\"bar\",\"value\":4},{\"key\":\"foo\",\"value\":3}]}}";
    PubsubMessageToTableRow.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

  @Test
  public void testAdditionalProperties() throws Exception {
    Map<String, Object> parent = new HashMap<>();
    Map<String, Object> additionalProperties = new HashMap<>();
    parent.put("outer", new HashMap<>(ImmutableMap.of("otherfield", 3, //
        "mapfield", new HashMap<>(ImmutableMap.of("foo", 3, "bar", 4)))));
    List<Field> bqFields = ImmutableList.of(Field.of("outer", LegacySQLTypeName.RECORD, //
        MAP_FIELD));
    String expected = "{\"outer\":{"
        + "\"mapfield\":[{\"key\":\"bar\",\"value\":4},{\"key\":\"foo\",\"value\":3}]}}";
    String expectedAdditional = "{\"outer\":{\"otherfield\":3}}";
    PubsubMessageToTableRow.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
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
    String expected = "{\"hi_fi\":true,\"_64bit\":true}";
    PubsubMessageToTableRow.transformForBqSchema(parent, bqFields, additionalProperties);
    assertEquals(expected, Json.asString(parent));
  }

}
