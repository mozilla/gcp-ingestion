/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.avro;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.junit.Test;

public class PubsubMessageRecordFormatterTest {

  @Test
  public void testFormatNull() throws IOException {
    byte[] data = Json.asBytes(ImmutableMap.of("test_null", NullNode.instance));
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields().name("test_null").type().nullType()
        .noDefault().endRecord();

    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(null, record.get("test_null"));
  }

  @Test
  public void testFormatFlatSchema() throws IOException {
    byte[] data = Json.asBytes(ImmutableMap.<String, Object>builder()//
        .put("test_bool", true).put("test_long", -7).put("test_double", 0.99)
        .put("test_string", "hello world") //
        .build());
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

  private byte[] generateNestedObject() throws IOException {
    // a tree with boolean leaves
    return Json.asBytes(ImmutableMap.of("shape",
        ImmutableMap.of("quadrilateral", ImmutableMap.builder() //
            .put("square", true) //
            .put("rectangle", true) //
            .put("rhombus", true) //
            .put("parallelogram", true) //
            .put("trapezoid", true) //
            .put("kite", true) //
            .build())));
  }

  @Test
  public void testFormatWithNestedObjectStruct() throws IOException {
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
  public void testFormatWithNestedObjectMap() throws IOException {
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
  public void testFormatArray() throws IOException {
    byte[] data = Json.asBytes(ImmutableMap.builder() //
        .put("test_array", ImmutableList.of(true, false, true)) //
        .build());
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
  public void testFormatNullableBoolean() throws IOException {
    byte[] data = Json.asBytes(ImmutableMap.builder() //
        .put("test_none", NullNode.instance) //
        .put("test_some", true) //
        .build());
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
  public void testFormatMissingRequiredFieldThrowsException() throws IOException {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = Json.asBytes(ImmutableMap.of("unused", NullNode.instance));
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().booleanType().noDefault() //
        .endRecord();
    formatter.formatRecord(message, schema);
  }

  @Test(expected = AvroTypeException.class)
  public void testFormatNullAsBooleanWithBooleanDefault() throws IOException {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = Json.asBytes(ImmutableMap.of("test", NullNode.instance));
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().booleanType().booleanDefault(false) //
        .endRecord();
    GenericRecord record = formatter.formatRecord(message, schema);

    // TODO: cast null the default value of false
    assertEquals(false, record.get("test"));
  }

  @Test
  public void testFormatMissingAsBooleanWithBooleanDefaultIsNull() throws IOException {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = Json.asBytes(ImmutableMap.of("unused", NullNode.instance));
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
  public void testFormatMissingAsBooleanWithNullDefault() throws IOException {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = Json.asBytes(ImmutableMap.of("unused", NullNode.instance));
    PubsubMessage message = new PubsubMessage(data, Collections.emptyMap());

    Schema schema = SchemaBuilder.record("root").fields() //
        .name("test").type().unionOf().nullType().and().booleanType().endUnion().nullDefault() //
        .endRecord();
    GenericRecord record = formatter.formatRecord(message, schema);

    assertEquals(null, record.get("test"));
  }

  @Test
  public void testFormatMissingMultipleAsBooleanWithNull() throws IOException {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = Json.asBytes(ImmutableMap.of("unused", NullNode.instance));
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
  public void testFormatEmptyObjectAsBooleanWithNull() throws IOException {
    PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    byte[] data = Json.asBytes(ImmutableMap.of());
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
  public void testFormatNullableObject() throws IOException {
    byte[] data = Json.asBytes(ImmutableMap.builder() //
        .put("test_none", NullNode.instance) //
        .put("test_some", ImmutableMap.of()).put("test_field", true) //
        .build()); //
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
  public void testFormatCastsFieldsToJsonString() throws IOException {
    byte[] data = Json.asBytes(ImmutableMap.builder() //
        .put("test_bool", true) //
        .put("test_long", -7) //
        .put("test_double", 0.99) //
        .put("test_string", "hello world") //
        .put("test_object", ImmutableMap.of("test_field", true)) //
        .put("test_array", ImmutableList.of(1, 2, 3)) //
        .build());
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
  public void testFormatCorrectsFieldNames() throws IOException {
    byte[] data = Json.asBytes(ImmutableMap.builder() //
        .put("test", true) //
        .put("test-hyphen", true) //
        .put("test.dot", true) //
        .put("-test-prefix-hyphen", true) //
        .put("$test-bad-symbol", true) //
        .put("0-test-prefix-number", true) //
        .build());
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
