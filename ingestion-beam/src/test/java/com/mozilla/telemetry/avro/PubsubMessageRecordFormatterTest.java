/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import org.junit.Test;

public class PubsubMessageRecordFormatterTest {

  @Test
  public void testFormatNull() {
    byte[] data = new JSONObject().put("test_null", JSONObject.NULL).toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields().name("test_null").type().nullType()
        .noDefault().endRecord();
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(record.get("test_null"), null);
  }

  @Test
  public void testFormatFlatSchema() {
    byte[] data = new JSONObject() //
        .put("test_bool", true).put("test_long", -7).put("test_double", 0.99)
        .put("test_string", "hello world") //
        .toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test_bool").type().booleanType().noDefault() //
        .name("test_long").type().longType().noDefault() //
        .name("test_double").type().doubleType().noDefault() //
        .name("test_string").type().stringType().noDefault() //
        .endRecord();

    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(record.get("test_bool"), true);
    assertEquals(record.get("test_long"), -7l);
    assertEquals(record.get("test_double"), 0.99);
    assertEquals(record.get("test_string").toString(), "hello world");
  }

  private byte[] generateNestedObject() {
    // a tree with boolean leaves
    return new JSONObject().put("shape", new JSONObject() //
        .put("quadrilateral", new JSONObject() //
            .put("square", true) //
            .put("rectangle", true) //
            .put("rhombus", true) //
            .put("parallelogram", true) //
            .put("trapezoid", true) //
            .put("kite", true))) //
        .toString().getBytes();
  }

  @Test
  public void testFormatWithNestedObjectStruct() {
    byte[] data = generateNestedObject();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder//
        .record("root").fields() //
        .name("shape").type().record("shape").fields() //
        .name("quadrilateral").type().record("quadrilateral").fields() //
        .name("square").type().booleanType().noDefault() //
        .name("rectangle").type().booleanType().noDefault() //
        .name("rhombus").type().booleanType().noDefault() //
        .name("parallelogram").type().booleanType().noDefault() //
        .name("trapezoid").type().booleanType().noDefault() //
        .name("kite").type().booleanType().noDefault() //
        .endRecord().noDefault() // quadrilateral
        .endRecord().noDefault() // shape
        .endRecord();

    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    // TODO: https://issues.apache.org/jira/browse/AVRO-1582
    assertEquals(record.get("shape.quadrilateral.rhombus"), true);
    assertEquals(record.get("shape.triangle"), null);
  }

  @Test
  public void testFormatWithNestedObjectMap() {
    byte[] data = generateNestedObject();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder //
        .record("root").fields() //
        .name("shape").type().record("shape").fields() //
        .name("quadrilateral").type().map().values().booleanType().noDefault() //
        .endRecord().noDefault() //
        .endRecord();

    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    Map<String, Boolean> quad = (Map<String, Boolean>) record.get("shape.quadrilateral");
    assertEquals(quad.size(), 6);
  }

  @Test
  public void testFormatCastsFieldsToJSONString() {
    byte[] data = new JSONObject() //
        .put("test_bool", true) //
        .put("test_long", -7) //
        .put("test_double", 0.99) //
        .put("test_string", "hello world") //
        .put("test_object", new JSONObject().put("test_field", true)) //
        .put("test_array", new JSONArray().put(1).put(2).put(3)) //
        .toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test_bool").type().stringType().noDefault() //
        .name("test_long").type().stringType().noDefault() //
        .name("test_double").type().stringType().noDefault() //
        .name("test_string").type().stringType().noDefault() //
        .name("test_object").type().stringType().noDefault() //
        .name("test_array").type().stringType().noDefault() //
        .endRecord();

    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(record.get("test_bool").toString(), "true");
    assertEquals(record.get("test_long").toString(), "-7");
    assertEquals(record.get("test_double").toString(), "0.99");
    assertEquals(record.get("test_string").toString(), "hello world");
    assertEquals(record.get("test_object").toString(), "{\"test_field\": true}");
    assertEquals(record.get("test_string").toString(), "[1, 2, 3]");
  }

  @Test
  public void testFormatCorrectsFieldNames() {
    byte[] data = new JSONObject() //
        .put("test", true) //
        .put("test-hyphen", true) //
        .put("test.dot", true) //
        .put("-test-prefix-hyphen", true) //
        .put("$test-bad-symbol", true) //
        .put("0-test-prefix-number", true) //
        .toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().booleanType().noDefault() //
        .name("test_hyphen").type().booleanType().noDefault() //
        .name("test_dot").type().booleanType().noDefault() //
        .name("_test_prefix_hyphen").type().booleanType().noDefault() //
        .name("_0_test_prefix_number").type().booleanType().noDefault() //
        .endRecord();

    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(record.get("test"), true);
    assertEquals(record.get("test_hyphen"), true);
    assertEquals(record.get("test_dot"), true);
    assertEquals(record.get("_test_prefix_hyphen"), true);
    assertEquals(record.get("_0_test_prefix_number"), true);
    assertEquals(record.get("$test_bad_symbol"), null);
  }
}
