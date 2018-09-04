/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class JsonToPubsubMessage extends PTransform<PCollection<String>, PCollectionTuple> {
  public TupleTag<PubsubMessage> mainTag = new TupleTag<PubsubMessage>();
  public TupleTag<PubsubMessage> errorTag = new TupleTag<PubsubMessage>();

  private class Fn extends DoFn<String, PubsubMessage> {
    private ObjectMapper objectMapper = new ObjectMapper()
        .addMixIn(PubsubMessage.class, PubsubMessageMixin.class);

    private PubsubMessage transform(String element) throws java.io.IOException {
      PubsubMessage output = objectMapper.readValue(element, PubsubMessage.class);
      if (output == null) {
        throw new NullPointerException();
      } else if (output.getPayload() == null) {
        throw new NullPointerException();
      }
      return output;
    }

    private PubsubMessage toError(String element, Throwable e) {
      // Create attributes map with required error fields
      Map<String, String> attributes = new HashMap<String, String>();
      attributes.put("error_type", this.toString());
      attributes.put("error_message", e.toString());
      // Return a new PubsubMessage
      return new PubsubMessage(element.getBytes(), attributes);
    }

    @ProcessElement
    public void processElement(@Element String element, MultiOutputReceiver out) {
      try {
        out.get(mainTag).output(transform(element));
      } catch (Throwable e) {
        out.get(errorTag).output(toError(element, e));
      }
    }
  }

  private Fn fn = new Fn();

  @Override
  public PCollectionTuple expand(PCollection<String> input) {
    PCollectionTuple output = input.apply(ParDo
        .of(fn)
        .withOutputTags(mainTag, TupleTagList.of(errorTag))
    );
    output.get(mainTag).setCoder(PubsubMessageWithAttributesCoder.of());
    output.get(errorTag).setCoder(PubsubMessageWithAttributesCoder.of());
    return output;
  }
}
