package com.mozilla.telemetry.transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class LimitPayloadSize extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  /** Exception to throw for messages whose payloads exceed the configured size limit. */
  public static class PayloadTooLargeException extends Exception {

    public PayloadTooLargeException(String message) {
      super(message);
    }
  }

  /** Return a transform that will send messages to error output if payload exceeds maxBytes. */
  public static LimitPayloadSize toBytes(int maxBytes) {
    return new LimitPayloadSize(maxBytes);
  }

  /** Return a transform that will send messages to error output if payload exceeds mb megabytes. */
  public static LimitPayloadSize toMB(int mb) {
    return new LimitPayloadSize(mb * 1024 * 1024);
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> elements) {
    return elements.apply(
        MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);
          int numBytes = message.getPayload().length;
          if (numBytes > maxBytes) {
            countPayloadTooLarge.inc();
            throw new RuntimeException(new PayloadTooLargeException("Message payload is " + numBytes
                + "bytes, larger than the" + " configured limit of " + maxBytes));
          }
          return message;
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
            .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> FailureMessage.of(this,
                ee.element(), ee.exception())));
  }

  ////////

  private final int maxBytes;

  private final Counter countPayloadTooLarge = Metrics.counter(LimitPayloadSize.class,
      "payload_too_large");

  private LimitPayloadSize(int maxBytes) {
    this.maxBytes = maxBytes;
  }
}
