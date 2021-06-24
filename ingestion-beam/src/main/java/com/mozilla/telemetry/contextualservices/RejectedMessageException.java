package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.metrics.KeyedCounter;

public class RejectedMessageException extends RuntimeException {

  RejectedMessageException(String message) {
    super(message);
  }

  RejectedMessageException(String message, String reason) {
    super(message);
    KeyedCounter.inc("rejected_" + reason);
  }
}
