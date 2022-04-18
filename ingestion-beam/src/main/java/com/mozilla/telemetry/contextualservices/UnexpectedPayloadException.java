package com.mozilla.telemetry.contextualservices;

public class UnexpectedPayloadException extends RuntimeException {

  UnexpectedPayloadException(String message) {
    super(message);
  }
}
