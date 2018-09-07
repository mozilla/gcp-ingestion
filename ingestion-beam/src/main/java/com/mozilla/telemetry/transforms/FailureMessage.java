package com.mozilla.telemetry.transforms;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

public class FailureMessage {
  public static PubsubMessage of(Object caller, String element, Throwable e) {
    return FailureMessage.of(caller, element.getBytes(), e);
  }

  public static PubsubMessage of(Object caller, byte[] element, Throwable e) {
    Map<String, String> attributes = ImmutableMap.of(
        "error_type", caller.toString(),
        "error_message", e.toString(),
        "stack_trace", Arrays.toString(e.getStackTrace())
    );
    return new PubsubMessage(element, attributes);
  }
}
