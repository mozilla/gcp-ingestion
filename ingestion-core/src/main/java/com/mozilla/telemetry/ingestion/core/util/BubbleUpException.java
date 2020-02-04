package com.mozilla.telemetry.ingestion.core.util;

/**
 * Special exception class that can be thrown from the body of a
 * {@link com.mozilla.telemetry.transforms.MapElementsWithErrors}
 * subclass to indicate that the pipeline should fail rather than sending the current message
 * to error output.
 */
public class BubbleUpException extends RuntimeException {

  public BubbleUpException(Throwable cause) {
    super(cause);
  }
}
