/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

public class LimitPayloadSize extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

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
  protected PubsubMessage processElement(PubsubMessage message) throws Exception {
    message = PubsubConstraints.ensureNonNull(message);
    int numBytes = message.getPayload().length;
    if (numBytes > maxBytes) {
      countPayloadTooLarge.inc();
      throw new PayloadTooLargeException("Message payload is " + numBytes + "bytes, larger than the"
          + " configured limit of " + maxBytes);
    }
    return message;
  }

  ////////

  private final int maxBytes;

  private final Counter countPayloadTooLarge = Metrics.counter(LimitPayloadSize.class,
      "payload_too_large");

  private LimitPayloadSize(int maxBytes) {
    this.maxBytes = maxBytes;
  }
}
