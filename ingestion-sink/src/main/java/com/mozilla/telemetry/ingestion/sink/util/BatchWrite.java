package com.mozilla.telemetry.ingestion.sink.util;

import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public abstract class BatchWrite<InputT, EncodedT, BatchKeyT, BatchResultT>
    implements Function<InputT, CompletableFuture<Void>> {

  protected final PubsubMessageToTemplatedString batchKeyTemplate;

  private final long maxBytes;
  private final int maxMessages;
  private final long maxDelayMillis;

  public BatchWrite(long maxBytes, int maxMessages, Duration maxDelay, String batchKeyTemplate) {
    this.maxBytes = maxBytes;
    this.maxMessages = maxMessages;
    this.maxDelayMillis = maxDelay.toMillis();
    this.batchKeyTemplate = new PubsubMessageToTemplatedString(batchKeyTemplate);
  }

  @VisibleForTesting
  public final ConcurrentMap<BatchKeyT, Batch> batches = new ConcurrentHashMap<>();

  @Override
  public CompletableFuture<Void> apply(InputT input) {
    AtomicReference<CompletableFuture<Void>> output = new AtomicReference<>();
    EncodedT encodedInput = encodeInput(input);
    batches.compute(getBatchKey(input), (batchKey, batch) -> {
      if (batch != null) {
        batch.add(encodedInput).ifPresent(output::set);
      }
      if (output.get() == null) {
        batch = getBatch(batchKey);
        // add first input before allowing batch
        batch.add(encodedInput).ifPresent(output::set);
        // allow batch to complete by timeout now that we have attempted to add the first item
        batch.init.complete(null);
      }
      return batch;
    });

    return Optional.ofNullable(output.get())
        .orElseThrow(() -> new IllegalArgumentException("Empty batch rejected input"));
  }

  public abstract class Batch {

    // block this batch from completing by timeout until this future is resolved
    final CompletableFuture<Void> init = new CompletableFuture<>();

    private final CompletableFuture<Void> full;
    private final CompletableFuture<BatchResultT> result;

    @VisibleForTesting
    public int size = 0;

    @VisibleForTesting
    public long byteSize = 0;

    public Batch() {
      // wait for init then setup full indicator by timeout
      full = init.thenRunAsync(this::timeout).exceptionally(ignore -> null);
      // wait for full then close
      result = full.thenComposeAsync(this::close);
    }

    private void timeout() {
      try {
        Thread.sleep(maxDelayMillis);
      } catch (InterruptedException e) {
        // this is fine
      }
    }

    private synchronized Optional<CompletableFuture<Void>> add(EncodedT encodedInput) {
      if (full.isDone()) {
        return Optional.empty();
      }
      int newSize = size + 1;
      long newByteSize = byteSize + getByteSize(encodedInput);
      if (newSize > maxMessages || newByteSize > maxBytes) {
        this.full.complete(null);
        return Optional.empty();
      }
      size = newSize;
      byteSize = newByteSize;
      write(encodedInput);
      return Optional
          .of(result.thenAcceptAsync(result -> this.checkResultFor(result, newSize - 1)));
    }

    protected void checkResultFor(BatchResultT batchResult, int index) {
    }

    protected abstract CompletableFuture<BatchResultT> close(Void ignore);

    protected abstract void write(EncodedT encodedInput);

    protected abstract long getByteSize(EncodedT encodedInput);
  }

  protected abstract BatchKeyT getBatchKey(InputT input);

  protected abstract EncodedT encodeInput(InputT input);

  protected abstract Batch getBatch(BatchKeyT batchKey);
}
