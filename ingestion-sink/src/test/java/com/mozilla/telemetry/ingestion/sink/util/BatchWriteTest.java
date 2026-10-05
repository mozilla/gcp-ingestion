package com.mozilla.telemetry.ingestion.sink.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import org.junit.Test;

public class BatchWriteTest {

  private static class NoopBatchWrite extends BatchWrite<String, String, String, Void> {

    int batchCount = 0;

    private NoopBatchWrite(long maxBytes, int maxMessages, Duration maxDelay) {
      super(maxBytes, maxMessages, maxDelay, null, ForkJoinPool.commonPool(), false);
    }

    @Override
    protected String encodeInput(String input) {
      return input;
    }

    @Override
    protected String getBatchKey(String input) {
      return input;
    }

    @Override
    protected synchronized Batch getBatch(String batchKey) {
      batchCount += 1;
      return new Batch();
    }

    class Batch extends BatchWrite<String, String, String, Void>.Batch {

      @Override
      protected CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(() -> {
        });
      }

      @Override
      protected void write(String encodedInput) {
      }

      @Override
      protected long getByteSize(String encodedInput) {
        return encodedInput.length();
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void canRejectFirstMessage() {
    NoopBatchWrite x = new NoopBatchWrite(0, 0, Duration.ofMillis(0)) {

      @Override
      protected Batch getBatch(String batchKey) {
        Batch batch = super.getBatch(batchKey);
        // getBatch normally won't return a full batch unless Batch.timeout() finishes in an async
        // thread before the original thread can call Batch.add(). That won't reliably occur for
        // this test, even with a maxDelay of 0, so forcibly complete Batch.full instead.
        batch.full.complete(null);
        return batch;
      }
    };
    x.apply("");
  }

  @Test
  public void canAcceptOversizeMessages() {
    NoopBatchWrite x = new NoopBatchWrite(0, 0, Duration.ofMillis(0));
    CompletableFuture.allOf(x.apply("x"), x.apply("x")).join();
    assertEquals(2, x.batchCount);
  }

  private static class ErrorBatchWrite extends NoopBatchWrite {

    private ErrorBatchWrite(long maxBytes, int maxMessages, Duration maxDelay) {
      super(maxBytes, maxMessages, maxDelay);
    }

    @Override
    protected synchronized Batch getBatch(String batchKey) {
      return new Batch() {

        @Override
        protected CompletableFuture<Void> close() {
          throw new OutOfMemoryError("test");
        }

        @Override
        protected String describe() {
          return "batch " + batchKey;
        }
      };
    }
  }

  private static Throwable joinCause(CompletableFuture<Void> future) {
    try {
      future.join();
    } catch (CompletionException e) {
      return e.getCause();
    }
    fail("expected batch to fail");
    return null;
  }

  @Test
  public void canFailSingleMessageWithError() {
    ErrorBatchWrite x = new ErrorBatchWrite(0, 0, Duration.ofMillis(0));
    Throwable cause = joinCause(x.apply("x"));
    assertTrue(cause instanceof OutOfMemoryError);
  }

  @Test
  public void canFailBatchWithError() {
    ErrorBatchWrite x = new ErrorBatchWrite(100, 100, Duration.ofMillis(100));
    CompletableFuture<Void> first = x.apply("x");
    final CompletableFuture<Void> second = x.apply("x");
    Throwable cause = joinCause(first);
    assertTrue(cause instanceof BatchException);
    assertEquals(2, ((BatchException) cause).size);
    assertEquals("batch x", ((BatchException) cause).description);
    assertTrue(cause.getCause() instanceof OutOfMemoryError);
    assertEquals(cause, joinCause(second));
  }
}
