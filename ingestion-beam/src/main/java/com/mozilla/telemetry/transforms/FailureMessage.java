/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * A collection of static methods for producing consistently-formatted PubsubMessage
 * packets for error output.
 */
public class FailureMessage {

  /**
   * Return a modified PubsubMessage with additional attributes describing the error.
   */
  public static PubsubMessage of(Object caller, PubsubMessage message, Throwable e) {
    final Map<String, String> attributes = new HashMap<>();
    if (message.getAttributeMap() != null) {
      attributes.putAll(message.getAttributeMap());
    }
    attributes.putAll(errorAttributes(caller, e));
    return new PubsubMessage(message.getPayload(), attributes);
  }

  /**
   * Return a PubsubMessage wrapping a String payload with attributes describing the error.
   */
  public static PubsubMessage of(Object caller, String payload, Throwable e) {
    return FailureMessage.of(caller, payload.getBytes(), e);
  }

  /**
   * Return a PubsubMessage wrapping a byte array payload with attributes describing the error.
   */
  public static PubsubMessage of(Object caller, byte[] payload, Throwable e) {
    return new PubsubMessage(payload, errorAttributes(caller, e));
  }

  private static Map<String, String> errorAttributes(Object caller, Throwable e) {
    return ImmutableMap.of(
        "error_type", caller.toString(),
        "error_message", e.toString(),
        "stack_trace", Arrays.toString(e.getStackTrace())
    );
  }
}
