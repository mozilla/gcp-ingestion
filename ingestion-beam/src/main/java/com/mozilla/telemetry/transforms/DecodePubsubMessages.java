/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

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

/**
 * Base class for decoding PubsubMessage from various input formats.
 *
 * <p>Subclasses are provided via static members fromText() and fromJson().
 * We also provide an alreadyDecoded() transform for creating a PCollectionTuple
 * analogous to fromText or fromJson to handle output from PubsubIO.
 *
 * <p>The packaging of subclasses here follows the style guidelines as captured in
 * https://beam.apache.org/contribute/ptransform-style-guide/#packaging-a-family-of-transforms
 */
public abstract class DecodePubsubMessages
    extends PTransform<PCollection<? extends String>, PCollectionTuple> {

  public static TupleTag<PubsubMessage> mainTag = new TupleTag<PubsubMessage>();
  public static TupleTag<PubsubMessage> errorTag = new TupleTag<PubsubMessage>();
  public static TupleTagList additionalOutputTags = TupleTagList.of(errorTag);

  // Force use of static factory methods.
  private DecodePubsubMessages() {}

  /*
   * Static factory methods.
   */

  /** Decoder from non-json text to PubsubMessage. */
  public static Text text() {
    return new Text();
  }

  /** Decoder from json to PubsubMessage. */
  public static Json json() {
    return new Json();
  }

  /**
   * Pass-through for pre-decoded PubsubMessages. It simply routes all inputs to the mainTag.
   *
   * <p>This is necessary to provide a uniform interface for different input sources.
   */
  public static AlreadyDecoded alreadyDecoded() {
    return new AlreadyDecoded();
  }

  /*
   * Concrete subclasses.
   */

  public static class Text extends DecodePubsubMessages {
    PubsubMessage transform(String element) {
      return new PubsubMessage(element.getBytes(), null);
    }
  }

  public static class Json extends DecodePubsubMessages {
    PubsubMessage transform(String element) throws IOException {
      PubsubMessage output = PubsubMessageMixin.MAPPER.readValue(element, PubsubMessage.class);
      if (output == null) {
        throw new NullPointerException();
      } else if (output.getPayload() == null) {
        throw new NullPointerException();
      }
      return output;
    }
  }

  public static class AlreadyDecoded
      extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {
    class Fn extends DoFn<PubsubMessage, PubsubMessage> {
      @ProcessElement
      public void processElement(@Element PubsubMessage element, MultiOutputReceiver out) {
        out.get(mainTag).output(element);
      }
    }

    @Override
    public PCollectionTuple expand(PCollection<PubsubMessage> input) {
      return input
          .apply(ParDo
              .of(new Fn())
              .withOutputTags(mainTag, additionalOutputTags));
    }
  }

  /*
   * Other methods and fields.
   */

  abstract PubsubMessage transform(String element) throws java.io.IOException;

  private class Fn extends DoFn<String, PubsubMessage> {
    @ProcessElement
    public void processElement(@Element String element, MultiOutputReceiver out) {
      try {
        out.get(mainTag).output(transform(element));
      } catch (Throwable e) {
        out.get(errorTag).output(FailureMessage.of(this, element, e));
      }
    }
  }

  private Fn fn = new Fn();

  @Override
  public PCollectionTuple expand(PCollection<? extends String> input) {
    PCollectionTuple output = input.apply(ParDo
        .of(fn)
        .withOutputTags(mainTag, additionalOutputTags)
    );
    output.get(mainTag).setCoder(PubsubMessageWithAttributesCoder.of());
    output.get(errorTag).setCoder(PubsubMessageWithAttributesCoder.of());
    return output;
  }
}
