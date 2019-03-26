package com.mozilla.telemetry.avro;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.io.ParsingDecoder;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

public class GuidedJsonDecoder extends ParsingDecoder implements Parser.ActionHandler {

  private JsonParser in;
  private static JsonFactory jsonFactory = new JsonFactory();
  private static ObjectMapper mapper = new ObjectMapper();

  GuidedJsonDecoder(Symbol root, InputStream in) throws IOException {
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

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    return null;
  }

  @Override
  protected void skipFixed() throws IOException {

  }

  @Override
  public void readNull() throws IOException {
    parser.advance(Symbol.NULL);
    in.nextToken();
  }

  @Override
  public boolean readBoolean() throws IOException {
    return false;
  }

  @Override
  public int readInt() throws IOException {
    return 0;
  }

  @Override
  public long readLong() throws IOException {
    return 0;
  }

  @Override
  public float readFloat() throws IOException {
    return 0;
  }

  @Override
  public double readDouble() throws IOException {
    return 0;
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return null;
  }

  @Override
  public String readString() throws IOException {
    // mapper.readTree(in);
    return null;
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