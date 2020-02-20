package com.mozilla.telemetry.ingestion.sink.util;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

public class BatchWriteTest {

  private static class NoopBatchWrite extends BatchWrite<String, String, String, Void> {

    int batchCount = 0;

    private NoopBatchWrite(long maxBytes, int maxMessages, Duration maxDelay) {
      super(maxBytes, maxMessages, maxDelay, null);
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
      protected CompletableFuture<Void> close(Void ignore) {
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
}
