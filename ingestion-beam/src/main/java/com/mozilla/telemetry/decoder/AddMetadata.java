/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.utils.Json;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class AddMetadata
    extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

  public static TupleTag<PubsubMessage> mainTag = new TupleTag<PubsubMessage>();
  public static TupleTag<PubsubMessage> errorTag = new TupleTag<PubsubMessage>();

  private static final byte[] METADATA_PREFIX = "{\"metadata\":".getBytes();

  private static PubsubMessage transform(PubsubMessage element) throws IOException {
    // Get payload
    final byte[] payload = element.getPayload();
    // Get attributes as bytes, throws IOException
    final byte[] metadata = Json.asBytes(element.getAttributeMap());
    // Ensure that we have a json object with no leading whitespace
    if (payload.length < 2 || payload[0] != '{') {
      throw new IOException("invalid json object: must start with {");
    }
    // Create an output stream for joining metadata with payload
    final ByteArrayOutputStream payloadWithMetadata =
        new ByteArrayOutputStream(METADATA_PREFIX.length + metadata.length + payload.length);
    // Write metadata prefix
    payloadWithMetadata.write(METADATA_PREFIX);
    // Write metadata
    payloadWithMetadata.write(metadata);
    // Start next json field, unless object was empty
    if (payload.length > 2) {
      // Write comma to start the next field
      payloadWithMetadata.write(',');
    }
    // Write payload without leading `{`
    payloadWithMetadata.write(payload, 1, payload.length - 1);
    return new PubsubMessage(payloadWithMetadata.toByteArray(), element.getAttributeMap());
  }

  private static class Fn extends DoFn<PubsubMessage, PubsubMessage> {
    @ProcessElement
    public void processElement(@Element PubsubMessage element, MultiOutputReceiver out) {
      try {
        out.get(mainTag).output(transform(element));
      } catch (Throwable e) {
        out.get(errorTag).output(
            FailureMessage.of(AddMetadata.class.getName(), element, e));
      }
    }
  }

  private static final Fn FN = new Fn();

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {
    PCollectionTuple output = input.apply(ParDo
        .of(FN)
        .withOutputTags(mainTag, TupleTagList.of(errorTag))
    );
    output.get(errorTag).setCoder(PubsubMessageWithAttributesCoder.of());
    return output;
  }
}
