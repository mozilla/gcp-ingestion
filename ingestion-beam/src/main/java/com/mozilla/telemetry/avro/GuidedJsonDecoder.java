/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.avro;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.ParsingDecoder;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

/**
 * A custom JSON decoder based on the Parsing JsonDecoder in the
 * `org.apache.avro.io.JsonDecoder` package.
 *
 * <p>The default JsonDecoder (as of Avro 1.8.2) doesn't support ingestion of JSON
 * documents where casting of values are inferred from the provided schema (see
 * AVRO-1582 [1]). Instead, the decoder expects values that are emitted by the
 * JsonEncoder in the form `{"NAME": {"TYPE": "VALUE"}}`. While this enables a
 * lossless type-encoding when using JSON as a transport, it is inconvenient for
 * ingesting data that are directly encoded into JSON i.e. values in the form
 * `{"NAME": "VALUE"}`.
 *
 * <p>This decoder is designed as a companion to the jsonschema-transpiler [2],
 * which emits valid Avro schemas from JSON Schema. These schemas are used to
 * guide the serialization process. This is required to disambiguate floating
 * points from integers and records from maps. To support BigQuery, the decoder
 * uses unions to enforce required fields and will fail on variant-types.
 * Additionally, field names are renamed to match the column-name rules for
 * BigQuery tables. The source JSON schemas can be underspecified or overly
 * complex. In such cases, the transpiler will cast fields into strings. This
 * encoder supports encoding tree-nodes into strings when guided by the provided
 * schema.
 *
 * <p>This class shares some code from the JsonDecoder class. The original class
 * does not allow protected access to the parser that drives the decoder. The
 * algorithm for buffering object fields in the streaming parser and general
 * coding style is borrowed from the source in [4]. One significant improvement
 * to code conciseness is to use Jackson's TokenBuffer to replay the JSON parser
 * when needed.
 *
 * <li> [1] https://issues.apache.org/jira/browse/AVRO-1582
 * <li> [2] https://github.com/acmiyaguchi/jsonschema-transpiler
 * <li> [3] https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro
 * <li> [4] https://github.com/apache/avro/blob/branch-1.8/lang/java/avro/src/main/java/org/apache/avro/io/JsonDecoder.java
 */
public class GuidedJsonDecoder extends ParsingDecoder implements Parser.ActionHandler {

  private JsonParser in;
  private static JsonFactory jsonFactory = new JsonFactory();
  private static ObjectMapper mapper = new ObjectMapper(jsonFactory);

  // A helper data-structure for random access to various fields. The generating
  // grammar is LL(1), so we can expect to re-read the tree several times
  // depending on the ordering of the document.
  Stack<Context> recordStack = new Stack<>();

  private static class Context {

    public Map<String, TokenBuffer> record = new HashMap<>();
    public JsonParser jp = null;
  }

  private GuidedJsonDecoder(Symbol root, InputStream in) throws IOException {
    super(root);
    configure(in);
  }

  GuidedJsonDecoder(Schema schema, final InputStream in) throws IOException {
    this(new JsonGrammarGenerator().generate(schema), in);
  }

  /**
   * Create a GuidedJsonDecoder from an InputStream.
   */
  public GuidedJsonDecoder configure(InputStream in) throws IOException {
    if (null == in) {
      throw new NullPointerException("InputStream to read cannot be null!");
    }
    parser.reset();
    this.in = jsonFactory.createParser(in);
    this.in.nextToken();
    return this;
  }

  private void error(JsonToken token, String type) throws AvroTypeException {
    throw new AvroTypeException("Expected " + type + ". Got " + token);
  }

  private void assertCurrentToken(JsonToken expect, String type) throws AvroTypeException {
    JsonToken token = in.getCurrentToken();
    if (token != expect) {
      error(token, type);
    }
  }

  private void assertCurrentTokenOneOf(JsonToken[] tokens, String type) throws AvroTypeException {
    JsonToken token = in.getCurrentToken();
    for (JsonToken expect : tokens) {
      if (token == expect) {
        return;
      }
    }
    error(token, type);
  }

  /**
   * Rename a field to conform to match the regex `^[a-zA-Z_][a-ZA-Z0-9_]*`. In
   * particular, replace periods and dashes with underscores and prefix names
   * that start with numbers.
   *
   * @param name the original field name in the JSON document
   * @return a formatted name
   */
  protected String renameField(String name) {
    String result = name.replace('.', '_').replace('-', '_');
    if (Character.isDigit(result.charAt(0))) {
      result = '_' + result;
    }
    return result;
  }

  /**
   * Implements the action handler for the Parser. Implicit actions allow the
   * encoder to maintain state and provides an entrypoint for modifying the
   * underlying stack machine.
   *
   * <p>The generating grammar is LL(1), reading the schema in a top-down,
   * left-most order. The ordering of fields in JSON is arbitrary, so the fields
   * must be cached to provide random access when using the streaming JsonParser
   * API. The TokenBuffer stores JsonTokens for replaying, which is more
   * efficient than reparsing or using the ObjectMapper.
   *
   * <p>An example of behavior can be visualized using a binary-tree. `[[1, [2,
   * 3]], [4, [5, 6]]` generates the sequence `[1, 2, 3, 4, 5, 6]` when read
   * top-down, left to right. This is the order that the schema is read in as.
   * If the document is serialized in the same order, then there is no need for
   * caching within the stack and the document is read in O(N) tokens. The
   * worst-case behavior occurs with this tree: `[[2, [6, 5]], [1, [3, 2]]]`.
   * Tracing the behavior reveals that complexity is O(N) in cached tokens and
   * O(2N) in the total number of read tokens when the tree is binary.
   *
   * <p>The context stack could be used to generate a view of the residue fields
   * given the right set of symbols and modification to the schema.
   */
  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top == Symbol.RECORD_START) {
      // Prepare for a new level of nesting by pushing onto the stack. The top
      // of the stack is the current depth in the document that the JsonParser
      // currently points to.
      assertCurrentToken(JsonToken.START_OBJECT, "record-start");
      recordStack.push(new Context());
      in.nextToken();
    } else if (top instanceof Symbol.FieldAdjustAction) {
      // Action the starts the process of reading data into a field as specified
      // by the DatumReader.
      Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
      Context ctx = recordStack.peek();

      // The decoder may have already parsed the field value of interest. It
      // performs a context switch and replays the cached content.
      TokenBuffer buffer = ctx.record.get(fa.fname);
      if (buffer != null) {
        ctx.record.remove(fa.fname);
        ctx.jp = in;
        in = buffer.asParser();
        in.nextToken();
        return null;
      }
      // The parser continues to stream the document, caching the raw tokens into
      // replayable buffers.
      if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
        do {
          String name = renameField(in.getValueAsString());
          in.nextToken();
          if (fa.fname.equals(name)) {
            return null;
          }
          // Make a copy of the current JSON structure, which moves the current
          // parser to the last event that was copied. Then increment to the
          // next FIELD_NAME.
          buffer = new TokenBuffer(in);
          buffer.copyCurrentStructure(in);
          ctx.record.put(name, buffer);
          in.nextToken();
        } while (in.getCurrentToken() == JsonToken.FIELD_NAME);
      }
      if (parser.topSymbol() == Symbol.UNION) {
        // Insert the default value for a missing, nullable type. The symbol
        // set for grammars does not include the default value from the
        // schema, so we are not able to insert anything other than null.
        // However, schemas generated by the schema-transpiler will use unions
        // to specify whether a field is nullable (not required). This means
        // that null values can be inserted when missing.
        buffer = new TokenBuffer(in);
        buffer.writeNull();
        buffer.close();

        // Swap the current parser and initialize the current token.
        ctx.jp = in;
        in = buffer.asParser();
        in.nextToken();

        return null;
      }
      throw new AvroTypeException("Expected field name not found: " + fa.fname);
    } else if (top == Symbol.FIELD_END) {
      // The decoder has reached the end of a field. We switch back into the
      // original parser and continue.
      Context ctx = recordStack.peek();
      if (ctx.jp != null) {
        in = ctx.jp;
        ctx.jp = null;
      }
    } else if (top == Symbol.RECORD_END) {
      // The decoder has read all of the fields that are specified in the
      // schema. It skips any remaining fields in the parser and then returns to
      // the previous level of nesting.
      while (in.getCurrentToken() != JsonToken.END_OBJECT) {
        in.nextToken();
      }
      recordStack.pop();
      in.nextToken();
    } else {
      throw new AvroTypeException("Unknown action symbol " + top);
    }
    return null;
  }

  @Override
  public void readNull() throws IOException {
    parser.advance(Symbol.NULL);
    assertCurrentToken(JsonToken.VALUE_NULL, "null");

    in.nextToken();
  }

  @Override
  public boolean readBoolean() throws IOException {
    parser.advance(Symbol.BOOLEAN);
    assertCurrentTokenOneOf(new JsonToken[] { JsonToken.VALUE_TRUE, JsonToken.VALUE_FALSE },
        "boolean");

    boolean result = in.getBooleanValue();
    in.nextToken();
    return result;
  }

  @Override
  public int readInt() throws IOException {
    parser.advance(Symbol.INT);
    assertCurrentTokenOneOf(
        new JsonToken[] { JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NUMBER_INT }, "int");

    int result = in.getIntValue();
    in.nextToken();
    return result;
  }

  @Override
  public long readLong() throws IOException {
    parser.advance(Symbol.LONG);
    assertCurrentTokenOneOf(
        new JsonToken[] { JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NUMBER_INT }, "long");

    long result = in.getLongValue();
    in.nextToken();
    return result;
  }

  @Override
  public float readFloat() throws IOException {
    parser.advance(Symbol.FLOAT);
    assertCurrentTokenOneOf(
        new JsonToken[] { JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NUMBER_INT }, "float");

    float result = in.getFloatValue();
    in.nextToken();
    return result;
  }

  @Override
  public double readDouble() throws IOException {
    parser.advance(Symbol.DOUBLE);
    assertCurrentTokenOneOf(
        new JsonToken[] { JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NUMBER_INT }, "double");

    double result = in.getDoubleValue();
    in.nextToken();
    return result;
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return new Utf8(readString());
  }

  /**
   * Read a string from the current location in parser.
   *
   * <p>This method differs from the original JsonDecoder by serializing all
   * structures captured by the current token into a JSON string. This enables
   * consistent behavior for handling variant types (e.g. field that can be a
   * boolean and a string) and for under-specified schemas.
   *
   * <p>This encoding is lossy because JSON strings are conflated with standard
   * strings. Consider the case where a number is decoded into a string. To
   * convert this Avro file back into the original JSON document, the encoder
   * must parse all strings as JSON and inline them into the tree. Now, if the
   * original JSON represents a JSON object as a string (e.g. `{"payload":
   * "{\"foo\":\"bar\"}"`), then the encoder will generate a new object that is
   * different from the original.
   *
   * <p>There are a few ways to avoid this if it is undesirable. One way is to use
   * a binary encoding for the JSON data such as BSON or base64. A second is to
   * normalize documents to avoid nested JSON encodings and to specify a schema
   * explictly to guide the proper typing.
   */
  @Override
  public String readString() throws IOException {
    parser.advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      assertCurrentToken(JsonToken.FIELD_NAME, "map-key");
    }

    String result = null;
    if (in.getCurrentToken() == JsonToken.VALUE_STRING
        || in.getCurrentToken() == JsonToken.FIELD_NAME) {
      result = in.getValueAsString();
    } else {
      // Does this create excessive garbage collection?
      TokenBuffer buffer = new TokenBuffer(in);
      buffer.copyCurrentStructure(in);
      result = mapper.readTree(buffer.asParser()).toString();
      buffer.close();
    }
    in.nextToken();
    return result;
  }

  @Override
  public void skipString() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    // JSON data rarely contains binary data due to the existence of control
    // characters. A generated schema should generally not ask for this field.
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipBytes() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    // Fixed-length are part of the avro spec, but are currently not being
    // generated by the schema transpiler in any form.
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipFixed(int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void skipFixed() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readEnum() throws IOException {
    // Enums are validated during ingestion, but are treated as strings when
    // loaded into BigQuery. Currently this operation is not supported since the
    // behavior is consistent in the end, but this could reduce the size of the
    // intermediary files.
    throw new UnsupportedOperationException();
  }

  @Override
  public long readArrayStart() throws IOException {
    parser.advance(Symbol.ARRAY_START);
    assertCurrentToken(JsonToken.START_ARRAY, "array-start");
    in.nextToken();

    if (in.getCurrentToken() == JsonToken.END_ARRAY) {
      parser.advance(Symbol.ARRAY_END);
      in.nextToken();
      return 0;
    }
    return 1;
  }

  @Override
  public long arrayNext() throws IOException {
    parser.advance(Symbol.ITEM_END);

    if (in.getCurrentToken() == JsonToken.END_ARRAY) {
      parser.advance(Symbol.ARRAY_END);
      in.nextToken();
      return 0;
    }
    return 1;
  }

  @Override
  public long skipArray() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long readMapStart() throws IOException {
    parser.advance(Symbol.MAP_START);
    assertCurrentToken(JsonToken.START_OBJECT, "map-start");
    in.nextToken();

    if (in.getCurrentToken() == JsonToken.END_OBJECT) {
      parser.advance(Symbol.MAP_END);
      in.nextToken();
      return 0;
    }
    return 1;
  }

  @Override
  public long mapNext() throws IOException {
    parser.advance(Symbol.ITEM_END);

    if (in.getCurrentToken() == JsonToken.END_OBJECT) {
      parser.advance(Symbol.MAP_END);
      in.nextToken();
      return 0;
    }
    return 1;
  }

  @Override
  public long skipMap() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Find the index in the union of the current variant.
   *
   * <p>This method only supports a single nullable type. Having more than a single
   * type is invalid in this case and will cause the decoder to panic. This
   * behavior is by design, since BigQuery does not support variant types in
   * columns. It is also inefficient to match sub-documents against various
   * types, given the streaming interface and bias towards performance.
   *
   * <p>Variants of non-null types are invalid. We enforce this by ensuring there
   * are no more than 2 elements and that at least one of them is null if there
   * are 2. Unions are required to be non-empty.
   *
   * <li> Ok: [null], [type], [null, type]
   * <li> Bad: [type, type], [null, type, type]
   */
  @Override
  public int readIndex() throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();

    int nullIndex = top.findLabel("null");
    int typeIndex = nullIndex == 0 ? 1 : 0;

    if ((nullIndex < 0 && top.size() == 2) || (top.size() > 2)) {
      throw new AvroTypeException("Variant types are not supported.");
    }

    int index = in.getCurrentToken() == JsonToken.VALUE_NULL ? nullIndex : typeIndex;
    parser.pushSymbol(top.getSymbol(index));
    return index;
  }

}
