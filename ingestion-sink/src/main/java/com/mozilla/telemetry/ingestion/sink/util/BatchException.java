package com.mozilla.telemetry.ingestion.sink.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Exception that impacts more than one input and should only be handled once. */
public class BatchException extends RuntimeException {

  public final int size;

  private BatchException(RuntimeException e, int size) {
    super(e);
    this.size = size;
  }

  /** Wrap e in a BatchException if it isn't already and it impacts multiple inputs. */
  public static RuntimeException of(RuntimeException e, int size) {
    if (e instanceof BatchException || size == 1) {
      return e;
    }
    return new BatchException(e, size);
  }

  private AtomicBoolean handled = new AtomicBoolean(false);

  /** Execute an action only the first time this is called. */
  public void handle(Consumer<BatchException> action) {
    if (handled.compareAndSet(false, true)) {
      action.accept(this);
    }
  }
}
