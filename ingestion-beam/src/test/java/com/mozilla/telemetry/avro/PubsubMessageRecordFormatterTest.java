/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.avro;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
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

    assertEquals(null, record.get("test_null"));
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

    assertEquals(true, record.get("test_bool"));
    assertEquals(-7L, record.get("test_long"));
    assertEquals(0.99, record.get("test_double"));
    assertEquals("hello world", record.get("test_string").toString());
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

    GenericRecord shape = (GenericRecord) record.get("shape");
    GenericRecord quad = (GenericRecord) shape.get("quadrilateral");
    assertEquals(true, quad.get("rhombus"));
    assertEquals(null, shape.get("triangle"));
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

    GenericRecord shape = (GenericRecord) record.get("shape");
    Map<String, Boolean> quad = (Map<String, Boolean>) shape.get("quadrilateral");
    assertEquals(6, quad.size());
  }

  @Test
  public void testFormatArray() {
    byte[] data = new JSONObject() //
        .put("test_array", new JSONArray().put(true).put(false).put(true)) //
        .toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder //
        .record("root").fields() //
        .name("test_array").type().array().items().booleanType().noDefault() //
        .endRecord();

    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(Arrays.asList(true, false, true),
        (GenericArray<Boolean>) record.get("test_array"));
  }

  @Test
  public void testFormatNullableBoolean() {
    byte[] data = new JSONObject() //
        .put("test_none", JSONObject.NULL) //
        .put("test_some", true) //
        .toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder //
        .record("root").fields() //
        .name("test_none").type().unionOf().nullType().and().booleanType().endUnion().noDefault() //
        .name("test_some").type().unionOf().nullType().and().booleanType().endUnion().noDefault() //
        .endRecord();

    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(null, record.get("test_none"));
    assertEquals(true, record.get("test_some"));
  }

  @Test(expected = AvroTypeException.class)
  public void testFormatMissingRequiredFieldThrowsException() {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = new JSONObject().put("unused", JSONObject.NULL).toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().booleanType().noDefault() //
        .endRecord();
    formatter.formatRecord(message, schema);
  }

  @Test(expected = AvroTypeException.class)
  public void testFormatNullAsBooleanWithBooleanDefault() {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = new JSONObject().put("test", JSONObject.NULL).toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().booleanType().booleanDefault(false) //
        .endRecord();
    GenericRecord record = formatter.formatRecord(message, schema);

    // TODO: cast null the default value of false
    assertEquals(false, record.get("test"));
  }

  @Test
  public void testFormatMissingAsBooleanWithBooleanDefaultIsNull() {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = new JSONObject().put("unused", JSONObject.NULL).toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().unionOf().booleanType().and().nullType().endUnion()
        .booleanDefault(true) //
        .endRecord();
    GenericRecord record = formatter.formatRecord(message, schema);

    // TODO: return the default value of `true`
    // NOTE: even if the schema has a default value, there currently isn't a way
    // to obtain the field's default through the symbols in the stack.
    // Therefore, a missing field will be filled in with a null.
    assertEquals(null, record.get("test"));
  }

  @Test
  public void testFormatMissingAsBooleanWithNullDefault() {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = new JSONObject().put("unused", JSONObject.NULL).toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().unionOf().nullType().and().booleanType().endUnion().nullDefault() //
        .endRecord();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(null, record.get("test"));
  }

  @Test
  public void testFormatMissingMultipleAsBooleanWithNull() {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = new JSONObject().put("unused", JSONObject.NULL).toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().unionOf().nullType().and().booleanType().endUnion().nullDefault() //
        .name("test2").type().unionOf().nullType().and().booleanType().endUnion().nullDefault() //
        .endRecord();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(null, record.get("test"));
    assertEquals(null, record.get("test2"));
  }

  @Test
  public void testFormatEmptyObjectAsBooleanWithNull() {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = new JSONObject().toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().unionOf().nullType().and().booleanType().endUnion().nullDefault() //
        .name("test2").type().unionOf().nullType().and().booleanType().endUnion().nullDefault() //
        .endRecord();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(null, record.get("test"));
    assertEquals(null, record.get("test2"));
  }

  @Test
  public void testFormatNullableObject() {
    byte[] data = new JSONObject() //
        .put("test_none", JSONObject.NULL) //
        .put("test_some", new JSONObject().put("test_field", true)) //
        .toString().getBytes();
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema subschema = SchemaBuilder.record("test_object").fields().name("test_field").type()
        .booleanType().noDefault().endRecord();
    Schema schema = SchemaBuilder //
        .record("root").fields() //
        .name("test_none").type().unionOf().nullType().and().type(subschema).endUnion().noDefault()
        .name("test_some").type().unionOf().nullType().and().type(subschema).endUnion().noDefault()
        .endRecord();

    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(null, record.get("test_none"));
    assertEquals(true, ((GenericRecord) record.get("test_some")).get("test_field"));
  }

  @Test
  public void testFormatCastsFieldsToJsonString() {
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

    assertEquals("true", record.get("test_bool").toString());
    assertEquals("-7", record.get("test_long").toString());
    assertEquals("0.99", record.get("test_double").toString());
    assertEquals("hello world", record.get("test_string").toString());
    assertEquals("{\"test_field\":true}", record.get("test_object").toString());
    assertEquals("[1,2,3]", record.get("test_array").toString());
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

    assertEquals(true, record.get("test"));
    assertEquals(true, record.get("test_hyphen"));
    assertEquals(true, record.get("test_dot"));
    assertEquals(true, record.get("_test_prefix_hyphen"));
    assertEquals(true, record.get("_0_test_prefix_number"));
    assertEquals(null, record.get("$test_bad_symbol"));
  }
}
