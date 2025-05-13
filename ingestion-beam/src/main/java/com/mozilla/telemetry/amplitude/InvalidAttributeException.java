package com.mozilla.telemetry.amplitude;

public class InvalidAttributeException extends RuntimeException {

  InvalidAttributeException(String message) {
    super(message);
  }

  InvalidAttributeException(String message, String reason) {
    super(message);
  }
}
