package com.mozilla.telemetry.posthog;

public class InvalidAttributeException extends RuntimeException {

  public InvalidAttributeException(String message) {
    super(message);
  }

  public InvalidAttributeException(String message, String reason) {
    super(message);
  }
}
