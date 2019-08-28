/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.util.DerivedAttributesMap;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.commons.text.StringSubstitutor;

public abstract class BatchWrite<InputT, BatchKeyT, BatchResultT>
    implements Function<InputT, CompletableFuture<Void>> {

  protected final String batchKeyTemplate;

  private final long maxBytes;
  private final int maxMessages;
  private final long maxDelayMillis;

  public BatchWrite(long maxBytes, int maxMessages, Duration maxDelay, String batchKeyTemplate) {
    this.maxBytes = maxBytes;
    this.maxMessages = maxMessages;
    this.maxDelayMillis = maxDelay.toMillis();
    this.batchKeyTemplate = batchKeyTemplate;
  }

  @VisibleForTesting
  public final ConcurrentMap<BatchKeyT, Batch> batches = new ConcurrentHashMap<>();

  @Override
  public CompletableFuture<Void> apply(InputT input) {
    AtomicReference<CompletableFuture<Void>> output = new AtomicReference<>();

    batches.compute(getBatchKey(input), (batchKey, batch) -> {
      if (batch != null) {
        batch.add(input).ifPresent(output::set);
      }
      if (output.get() == null) {
        batch = getBatch(batchKey);
        // add first input before allowing batch
        batch.add(input).ifPresent(output::set);
        // allow batch to complete by timeout now that we have attempted to add the first item
        batch.init.complete(null);
      }
      return batch;
    });

    return Optional.ofNullable(output.get())
        .orElseThrow(() -> new IllegalArgumentException("Empty batch rejected input"));
  }

  protected String batchKeyTemplate(PubsubMessage input) {
    String batchKey = StringSubstitutor.replace(batchKeyTemplate,
        DerivedAttributesMap.of(input.getAttributesMap()));
    if (batchKey.contains("$")) {
      throw new IllegalArgumentException("Element did not contain all the attributes needed to"
          + " fill out variables in the configured output template: " + batchKeyTemplate);
    }
    return batchKey;
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
      full = init.thenRunAsync(this::timeout);
      // wait for full then close
      result = full.handleAsync(this::close);
    }

    private void timeout() {
      try {
        Thread.sleep(maxDelayMillis);
      } catch (InterruptedException e) {
        // this is fine
      }
    }

    private synchronized Optional<CompletableFuture<Void>> add(InputT input) {
      if (full.isDone()) {
        return Optional.empty();
      }
      int newSize = size + 1;
      long newByteSize = byteSize + getByteSize(input);
      if (newSize > maxMessages || newByteSize > maxBytes) {
        this.full.complete(null);
        return Optional.empty();
      }
      size = newSize;
      byteSize = newByteSize;
      write(input);
      return Optional
          .of(result.thenAcceptAsync(result -> this.checkResultFor(result, newSize - 1)));
    }

    protected void checkResultFor(BatchResultT batchResult, int index) {
    }

    protected abstract BatchResultT close(Void ignoreVoid, Throwable ignoreThrowable);

    protected abstract void write(InputT input);

    protected abstract long getByteSize(InputT input);
  }

  protected abstract BatchKeyT getBatchKey(InputT input);

  protected abstract Batch getBatch(BatchKeyT batchKey);
}
