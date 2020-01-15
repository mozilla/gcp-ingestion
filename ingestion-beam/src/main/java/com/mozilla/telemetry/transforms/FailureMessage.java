package com.mozilla.telemetry.transforms;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * A collection of static methods for producing consistently-formatted PubsubMessage
 * packets for error output.
 */
public class FailureMessage {

  private static final int MAX_STACK_TRACE_CAUSE_ATTRIBUTES = 5;

  /**
   * Return a modified PubsubMessage with additional attributes describing the error.
   */
  public static PubsubMessage of(Object caller, PubsubMessage message, Throwable e) {
    final Map<String, String> attributes = new HashMap<>();
    if (message.getAttributeMap() != null) {
      attributes.putAll(message.getAttributeMap());
    }
    attributes.putAll(errorAttributes(caller, e));
    return new PubsubMessage(message.getPayload(), attributes, message.getMessageId());
  }

  /**
   * Return a PubsubMessage wrapping a String payload with attributes describing the error.
   */
  public static PubsubMessage of(Object caller, String payload, Throwable e) {
    return FailureMessage.of(caller, payload.getBytes(StandardCharsets.UTF_8), e);
  }

  /**
   * Return a PubsubMessage wrapping a byte array payload with attributes describing the error.
   */
  public static PubsubMessage of(Object caller, byte[] payload, Throwable e) {
    return new PubsubMessage(payload, errorAttributes(caller, e), null);
  }

  /**
   * Return a PubsubMessage corresponding to an error reading from a file.
   */
  public static PubsubMessage of(Object caller, ReadableFile readableFile, Throwable e) {
    Map<String, String> attributes = errorAttributes(caller, e);
    attributes.put("readable_file", readableFile.toString());
    return new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributes, null);
  }

  private static Map<String, String> errorAttributes(Object caller, Throwable e) {
    Map<String, String> attributes = stackTraceAttributes(e);
    attributes.putAll(
        ImmutableMap.of("error_type", PubsubConstraints.truncateAttributeValue(caller.toString()),
            "error_message", PubsubConstraints.truncateAttributeValue(e.toString()),
            "exception_class", PubsubConstraints.truncateAttributeValue(e.getClass().getName())));
    return attributes;
  }

  private static String truncatedStackTrace(Throwable e) {
    if (e == null) {
      return null;
    } else {
      return PubsubConstraints.truncateAttributeValue(Arrays.toString(e.getStackTrace()));
    }
  }

  private static Map<String, String> stackTraceAttributes(Throwable e) {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("stack_trace", truncatedStackTrace(e));
    for (int i = 1; i <= MAX_STACK_TRACE_CAUSE_ATTRIBUTES; i++) {
      e = e.getCause();
      if (e == null) {
        return attributes;
      }
      attributes.put("stack_trace_cause_" + i, truncatedStackTrace(e));
    }
    return attributes;
  }

}
