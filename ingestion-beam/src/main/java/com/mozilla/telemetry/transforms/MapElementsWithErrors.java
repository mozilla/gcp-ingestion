package com.mozilla.telemetry.transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
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
    extends PTransform<PCollection<InputT>, WithErrors.Result<PCollection<OutputT>>> {

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
   * @throws Exception if the element should go into the {@code errorTag}.
   */
  protected abstract OutputT processElement(InputT element) throws Exception;

  /**
   * Method that returns one error PubsubMessage from an exception thrown by {@code processElement}.
   *
   * <p>The output of this method goes into the {@code errorTag}.
   *
   * @param element that caused {@code processElement} to throw an exception.
   * @param e exception thrown by {@code processElement}.
   * @return a {@link PubsubMessage} that holds {@code element} and {@code e}.
   */
  protected abstract PubsubMessage processError(InputT element, Exception e);

  /**
   * Default processError method for {@code InputT} == {@link PubsubMessage}.
   */
  protected PubsubMessage processError(PubsubMessage element, Exception e) {
    return FailureMessage.of(this, element, e);
  }

  /**
   * Default processError method for {@code InputT} == {@link String}.
   */
  protected PubsubMessage processError(String element, Exception e) {
    return FailureMessage.of(this, element, e);
  }

  /**
   * Default processError method for {@code InputT} == {@code byte[]}.
   */
  protected PubsubMessage processError(byte[] element, Exception e) {
    return FailureMessage.of(this, element, e);
  }

  /**
   * DoFn that redirects errors to {@code errorTag}.
   */
  private class DoFnWithErrors extends DoFn<InputT, OutputT> {

    @ProcessElement
    public void processElementOrError(@Element InputT element, MultiOutputReceiver out)
        throws BubbleUpException {
      OutputT processed = null;
      boolean exceptionWasThrown = false;
      try {
        processed = processElement(element);
      } catch (MessageShouldBeDroppedException e) {
        // Don't emit the message to any output.
        exceptionWasThrown = true;
      } catch (BubbleUpException e) {
        throw e;
      } catch (Exception e) {
        exceptionWasThrown = true;
        out.get(errorTag).output(processError(element, e));
      }
      if (!exceptionWasThrown) {
        out.get(successTag).output(processed);
      }
    }
  }

  /**
   * Singleton of {@link DoFnWithErrors}.
   */
  private final DoFnWithErrors fn = new DoFnWithErrors();

  @Override
  public WithErrors.Result<PCollection<OutputT>> expand(PCollection<InputT> input) {
    PCollectionTuple tuple = input
        .apply(ParDo.of(fn).withOutputTags(successTag, TupleTagList.of(errorTag)));
    return WithErrors.Result.of(tuple.get(successTag), successTag, tuple.get(errorTag), errorTag);
  }

  /**
   * Define {@link MapElementsWithErrors} for {@code OutputT} of {@link PubsubMessage}.
   */
  public abstract static class ToPubsubMessageFrom<InputT>
      extends MapElementsWithErrors<InputT, PubsubMessage> {

    @Override
    public WithErrors.Result<PCollection<PubsubMessage>> expand(PCollection<InputT> input) {
      WithErrors.Result<PCollection<PubsubMessage>> result = super.expand(input);
      result.output().setCoder(PubsubMessageWithAttributesAndMessageIdCoder.of());
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

  /**
   * Special exception class that implementations of {@link MapElementsWithErrors} can throw to
   * signal that a given message should not be sent downstream to either success or error output.
   */
  public static class MessageShouldBeDroppedException extends Exception {
  }

  /**
   * Special exception class that can be thrown from the body of a {@link MapElementsWithErrors}
   * subclass to indicate that the pipeline should fail rather than sending the current message
   * to error output.
   */
  public static class BubbleUpException extends RuntimeException {

    public BubbleUpException(Throwable cause) {
      super(cause);
    }
  }

}
