/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

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
 * {@link PTransform} with error handling and implementing {@code expand} for cases where each
 * element maps to one output in either {@code mainTag} or {@code errorTag}.
 *
 * <p>Provides default implementations of {@code processError} for some values of {@code InputT}.
 *
 * @param <InputT> type of elements in the input {@link PCollection}.
 * @param <OutputT> type of elements in the output {@link PCollection} for {@code mainTag}.
 */
public abstract class MapElementsWithErrors<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, ResultWithErrors<PCollection<OutputT>>> {

  private final TupleTag<OutputT> successTag = new TupleTag<OutputT>() {
  };
  private final TupleTag<PubsubMessage> errorTag = new TupleTag<PubsubMessage>() {
  };

  /**
   * Method that returns one instance of {@code OutputT} for each {@code element}.
   *
   * <p>The output of this method goes into the {@code mainTag}. If an exception is thrown, it is
   * formatted by {@code processError} and goes into the {@code errorTag}.
   *
   * @param element that should be processed.
   * @return an instance of {@code OutputT} that goes into {@code mainTag}.
   * @throws Throwable if the element should go into the {@code errorTag}.
   */
  protected abstract OutputT processElement(InputT element) throws Throwable;

  /**
   * Method that returns one error PubsubMessage from an exception thrown by {@code processElement}.
   *
   * <p>The output of this method goes into the {@code errorTag}.
   *
   * @param element that caused {@code processElement} to throw an exception.
   * @param e exception thrown by {@code processElement}.
   * @return a {@link PubsubMessage} that holds {@code element} and {@code e}.
   */
  protected abstract PubsubMessage processError(InputT element, Throwable e);

  /**
   * Default processError method for {@code InputT} == {@link PubsubMessage}.
   */
  protected PubsubMessage processError(PubsubMessage element, Throwable e) {
    return FailureMessage.of(this, element, e);
  }

  /**
   * Default processError method for {@code InputT} == {@link String}.
   */
  protected PubsubMessage processError(String element, Throwable e) {
    return FailureMessage.of(this, element, e);
  }

  /**
   * Default processError method for {@code InputT} == {@code byte[]}.
   */
  protected PubsubMessage processError(byte[] element, Throwable e) {
    return FailureMessage.of(this, element, e);
  }

  /**
   * DoFn that redirects errors to {@code errorTag}.
   */
  private class DoFnWithErrors extends DoFn<InputT, OutputT> {

    @ProcessElement
    public void processElementOrError(@Element InputT element, MultiOutputReceiver out) {
      try {
        out.get(successTag).output(processElement(element));
      } catch (Throwable e) {
        out.get(errorTag).output(processError(element, e));
      }
    }
  }

  /**
   * Singleton of {@link DoFnWithErrors}.
   */
  private final DoFnWithErrors fn = new DoFnWithErrors();

  @Override
  public ResultWithErrors<PCollection<OutputT>> expand(PCollection<? extends InputT> input) {
    PCollectionTuple tuple = input
        .apply(ParDo.of(fn).withOutputTags(successTag, TupleTagList.of(errorTag)));
    return ResultWithErrors.of(tuple.get(successTag), successTag, tuple.get(errorTag), errorTag);
  }

  /**
   * Define {@link MapElementsWithErrors} for {@code OutputT} of {@link PubsubMessage}.
   */
  public abstract static class ToPubsubMessageFrom<InputT>
      extends MapElementsWithErrors<InputT, PubsubMessage> {

    @Override
    public ResultWithErrors<PCollection<PubsubMessage>> expand(
        PCollection<? extends InputT> input) {
      ResultWithErrors<PCollection<PubsubMessage>> result = super.expand(input);
      result.output().setCoder(PubsubMessageWithAttributesCoder.of());
      return result;
    }

    /*
     * Static factory methods for subclasses.
     */

    public static Identity identity() {
      return new Identity();
    }

    /*
     * Static subclasses.
     */

    public static class Identity extends ToPubsubMessageFrom<PubsubMessage> {

      protected PubsubMessage processElement(PubsubMessage element) {
        return element;
      }
    }
  }
}
