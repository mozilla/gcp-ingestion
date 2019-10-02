package com.mozilla.telemetry.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Decodes an incoming PubsubMessage that contains an avro encoded payload that
 * is registered in the schema store.
 */
public class GenericRecordBinaryEncoder implements Serializable {

  /**
   * Encode a GenericRecord into an avro encoded payload.
   */
  public byte[] encodeRecord(GenericRecord record, Schema schema) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      writer.write(record, encoder);
      encoder.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
