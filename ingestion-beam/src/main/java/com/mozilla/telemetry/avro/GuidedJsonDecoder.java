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
          String name = in.getValueAsString();
          in.nextToken();
          if (fa.fname == name) {
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
  protected void skipFixed() throws IOException {

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
    String result = in.getValueAsString();
    in.nextToken();
    return result;
  }

  @Override
  public void skipString() throws IOException {

  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    return null;
  }

  @Override
  public void skipBytes() throws IOException {

  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {

  }

  @Override
  public void skipFixed(int length) throws IOException {

  }

  @Override
  public int readEnum() throws IOException {
    return 0;
  }

  @Override
  public long readArrayStart() throws IOException {
    return 0;
  }

  @Override
  public long arrayNext() throws IOException {
    return 0;
  }

  @Override
  public long skipArray() throws IOException {
    return 0;
  }

  @Override
  public long readMapStart() throws IOException {
    return 0;
  }

  @Override
  public long mapNext() throws IOException {
    return 0;
  }

  @Override
  public long skipMap() throws IOException {
    return 0;
  }

  @Override
  public int readIndex() throws IOException {
    return 0;
  }

}