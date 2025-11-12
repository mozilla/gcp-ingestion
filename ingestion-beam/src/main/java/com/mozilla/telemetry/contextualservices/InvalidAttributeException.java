package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.metrics.KeyedCounter;

public class InvalidAttributeException extends RuntimeException {

  InvalidAttributeException(String message) {
    super(message);
  }

  InvalidAttributeException(String message, String reason) {
    super(message);
    KeyedCounter.inc("invalid_attribute_" + reason);
  }
}
