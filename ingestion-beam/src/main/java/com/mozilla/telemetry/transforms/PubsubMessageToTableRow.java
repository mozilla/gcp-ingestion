/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class PubsubMessageToTableRow
    extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {
  public TupleTag<TableRow> mainTag = new TupleTag<TableRow>();
  public TupleTag<PubsubMessage> errorTag = new TupleTag<PubsubMessage>();

  private class Fn extends DoFn<PubsubMessage, TableRow> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private TableRow transform(PubsubMessage element) throws java.io.IOException {
      TableRow output = objectMapper.readValue(element.getPayload(), TableRow.class);
      if (output == null) {
        throw new NullPointerException();
      }
      return output;
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage element, MultiOutputReceiver out) {
      try {
        out.get(mainTag).output(transform(element));
      } catch (Throwable e) {
        out.get(errorTag).output(FailureMessage.of(this, element, e));
      }
    }
  }

  private Fn fn = new Fn();

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {
    PCollectionTuple output = input.apply(ParDo
        .of(fn)
        .withOutputTags(mainTag, TupleTagList.of(errorTag))
    );
    output.get(errorTag).setCoder(PubsubMessageWithAttributesCoder.of());
    return output;
  }
}
