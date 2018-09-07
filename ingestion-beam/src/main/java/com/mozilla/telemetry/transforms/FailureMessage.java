package com.mozilla.telemetry.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * A collection of static methods for producing consistently-formatted PubsubMessage
 * packets for error output.
 */
public class FailureMessage {

  /**
   * Return a PubsubMessage wrapping a String payload with attributes describing the error.
   */
  public static PubsubMessage of(Object caller, PubsubMessage payload, Throwable e) {
    try {
      return FailureMessage.of(caller, PubsubMessageMixin.MAPPER.writeValueAsBytes(payload), e);
    } catch (JsonProcessingException jsonException) {
      throw new RuntimeException(
          "Unexpected failure to encode a PubsubMessage to bytes", jsonException);
    }
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
    Map<String, String> attributes = ImmutableMap.of(
        "error_type", caller.toString(),
        "error_message", e.toString(),
        "stack_trace", Arrays.toString(e.getStackTrace())
    );
    return new PubsubMessage(payload, attributes);
  }
}
