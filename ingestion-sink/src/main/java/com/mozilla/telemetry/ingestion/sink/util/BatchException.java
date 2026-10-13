package com.mozilla.telemetry.ingestion.sink.util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Exception that impacts more than one input and should only be handled once. */
public class BatchException extends RuntimeException {

  public final int size;

  /** What the batch writes to, such as a file or table, for logs. May be null. */
  public final String description;

  private BatchException(Throwable e, int size, String description) {
    super(e);
    this.size = size;
    this.description = description;
  }

  /**
   * Same as {@link #of(Throwable, int, String)} with no description.
   */
  public static RuntimeException of(Throwable e, int size) {
    return of(e, size, null);
  }

  /**
   * Wrap e in a BatchException with a description of the batch, if it isn't already and it
   * impacts multiple inputs.
   *
   * <p>If e impacts a single input and is not a RuntimeException, such as an OutOfMemoryError, it
   * is wrapped in a CompletionException. CompletableFuture passes that through unchanged, so
   * callers still get e as the cause.
   */
  public static RuntimeException of(Throwable e, int size, String description) {
    if (e instanceof BatchException) {
      return (BatchException) e;
    }
    if (size != 1) {
      return new BatchException(e, size, description);
    }
    return e instanceof RuntimeException ? (RuntimeException) e : new CompletionException(e);
  }

  private AtomicBoolean handled = new AtomicBoolean(false);

  /** Execute an action only the first time this is called. */
  public void handle(Consumer<BatchException> action) {
    if (handled.compareAndSet(false, true)) {
      action.accept(this);
    }
  }
}
