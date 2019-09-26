/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.avro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.beam.sdk.io.AvroIO.RecordFormatter;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * Encodes an incoming message into a generic avro record.
*/
public class PubsubMessageRecordFormatter implements RecordFormatter<PubsubMessage> {

  @Override
  public GenericRecord formatRecord(PubsubMessage element, Schema schema) {
    GenericRecord result = null;
    try {
      InputStream in = new ByteArrayInputStream(element.getPayload());
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      Decoder decoder = new GuidedJsonDecoder(schema, in);
      result = reader.read(null, decoder);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (AvroTypeException e) {
      throw e;
    }
    return result;
  }
}
