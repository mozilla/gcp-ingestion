package com.mozilla.telemetry.avro;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.ParsingDecoder;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

public class GuidedJsonDecoder extends ParsingDecoder implements Parser.ActionHandler {

  private JsonParser in;
  private static JsonFactory jsonFactory = new JsonFactory();
  private static ObjectMapper mapper = new ObjectMapper(jsonFactory);

  // A helper data-structure for random access to various fields. The generating grammar
  // is LL(1), so we can expect to re-read the tree several times depending on the ordering
  // of the document. We could also serialize the tree directly -- we would need to keep
  // track of the current element in the tree however.
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

  protected String renameField(String name) {
    String result = name.replace('.', '_').replace('-', '_');
    if (Character.isDigit(result.charAt(0))) {
      result = '_' + result;
    }
    return result;
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top == Symbol.RECORD_START) {
      assertCurrentToken(JsonToken.START_OBJECT, "record-start");
      // Create a new layer of context
      recordStack.push(new Context());
      in.nextToken();
    } else if (top instanceof Symbol.FieldAdjustAction) {
      Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
      Context ctx = recordStack.peek();

      TokenBuffer buffer = ctx.record.get(fa.fname);
      // We already read the field, context switch to it
      if (buffer != null) {
        ctx.record.remove(fa.fname);
        ctx.jp = in;
        in = buffer.asParser();
        in.nextToken();
        return null;
      }
      // Keep streaming the document, caching fields that aren't relevant now
      if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
        do {
          String name = renameField(in.getValueAsString());
          in.nextToken();
          if (fa.fname.equals(name)) {
            return null;
          }
          // Make a copy of the current structure, which moves the current token
          // stream to the last event that was copied. Then increment to the
          // next FIELD_NAME.
          buffer = new TokenBuffer(in);
          buffer.copyCurrentStructure(in);
          ctx.record.put(name, buffer);
          in.nextToken();
        } while (in.getCurrentToken() == JsonToken.FIELD_NAME);
        throw new AvroTypeException("Expected field name not found: " + fa.fname);
      }
    } else if (top == Symbol.FIELD_END) {
      // Context switch to the original json parser
      Context ctx = recordStack.peek();
      if (ctx.jp != null) {
        in = ctx.jp;
        ctx.jp = null;
      }
    } else if (top == Symbol.RECORD_END) {
      // Find the end of the object and return to the last saved context
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

  @Override
  public int readIndex() throws IOException {
    // The schema transpiler collapses all uses of unions that specify variant
    // types. The remaining unions are used to distinguish required/nullable
    // fields in records.
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();

    int null_index = top.findLabel("null");
    int type_index = null_index == 0 ? 1 : 0;

    // Variants of concrete types (non-null) are invalid. We enforce this by
    // ensuring there are no more than 2 elements and that at least one of them
    // is null if there are 2. Unions are required to be non-empty.
    // Ok: [null], [type], [null, type]
    // Bad: [type, type], [null, type, type]
    if ((null_index < 0 && top.size() == 2) || (top.size() > 2)) {
      throw new AvroTypeException("Variant types are not supported.");
    }

    int index = in.getCurrentToken() == JsonToken.VALUE_NULL ? null_index : type_index;
    parser.pushSymbol(top.getSymbol(index));
    return index;
  }

}