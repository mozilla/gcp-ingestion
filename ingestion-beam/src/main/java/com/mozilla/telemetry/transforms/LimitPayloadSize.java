package com.mozilla.telemetry.transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.MapElements.MapWithFailures;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.values.TypeDescriptor;

public class LimitPayloadSize {

  /** Exception to throw for messages whose payloads exceed the configured size limit. */
  public static class PayloadTooLargeException extends RuntimeException {

    public PayloadTooLargeException(String message) {
      super(message);
    }
  }

  /** Return a transform that will send messages to error output if payload exceeds maxBytes. */
  public static MapWithFailures<PubsubMessage, PubsubMessage, PubsubMessage> toBytes(int maxBytes) {
    return LimitPayloadSize.of(maxBytes);
  }

  /** Return a transform that will send messages to error output if payload exceeds mb megabytes. */
  public static MapWithFailures<PubsubMessage, PubsubMessage, PubsubMessage> toMB(int mb) {
    return LimitPayloadSize.of(mb * 1024 * 1024);
  }

  /** Factory method to create mapper instance. */
  public static MapWithFailures<PubsubMessage, PubsubMessage, PubsubMessage> of(int maxBytes) {
    final Counter countPayloadTooLarge = Metrics.counter(LimitPayloadSize.class,
        "payload_too_large");
    return MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage msg) -> {
      msg = PubsubConstraints.ensureNonNull(msg);
      int numBytes = msg.getPayload().length;
      if (numBytes > maxBytes) {
        countPayloadTooLarge.inc();
        throw new PayloadTooLargeException("Message payload is " + numBytes
            + "bytes, larger than the" + " configured limit of " + maxBytes);
      }
      return msg;
    }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (PayloadTooLargeException e) {
            return FailureMessage.of(LimitPayloadSize.class.getSimpleName(), ee.element(),
                ee.exception());
          }
        });
  }
}
