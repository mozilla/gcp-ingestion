/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.transform;

import com.mozilla.telemetry.ingestion.util.KV;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class FileGroupByKey<KeyType, OutputType>
    implements Function<KV<KeyType, String>, CompletableFuture<OutputType>> {

  private final Map<KeyType, Batch> batches = new HashMap<>();
  private final int maxBytes;
  private final long maxDelayMillis;
  private final int maxMessages;
  private final Function<KV<KeyType, File>, CompletableFuture<OutputType>> handler;

  public FileGroupByKey(int maxBytes, Duration maxDelay, int maxMessages,
      Function<KV<KeyType, File>, CompletableFuture<OutputType>> handler) {
    this.maxBytes = maxBytes;
    maxDelayMillis = maxDelay.toMillis();
    this.maxMessages = maxMessages;
    this.handler = handler;
  }

  synchronized public CompletableFuture<OutputType> apply(KV<KeyType, String> kv) {
    return Optional.ofNullable(batches.get(kv.key)).flatMap(b -> b.add(kv.value)).orElseGet(() -> {
      final Batch batch = new Batch(kv.key, handler);
      final CompletableFuture<OutputType> response = batch.add(kv.value)
          .orElseThrow(() -> new IllegalArgumentException("Empty batch rejected message"));
      batches.put(kv.key, batch);
      return response;
    });
  }

  private class Batch {

    private final KeyType key;
    private final File file;
    private final BufferedWriter writer;
    private final CompletableFuture<Void> full;
    private final CompletableFuture<OutputType> result;

    int size = 0;
    int byteSize = 0;

    private Batch(KeyType key, Function<KV<KeyType, File>, CompletableFuture<OutputType>> handler) {
      this.key = key;
      try {
        file = File.createTempFile("", "");
        // TODO compress file
        writer = new BufferedWriter(new FileWriter(file));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      full = CompletableFuture.runAsync(this::timeout);
      result = full.handleAsync(this::output).thenComposeAsync(handler);
    }

    private void timeout() {
      try {
        Thread.sleep(maxDelayMillis);
      } catch (InterruptedException e) {
        // this is fine
      }
    }

    synchronized private KV<KeyType, File> output(Void v, Throwable t) {
      return new KV<>(key, file);
    }

    synchronized private Optional<CompletableFuture<OutputType>> add(String message) {
      if (full.isDone()) {
        return Optional.empty();
      }
      if (size > 0 && (size + 1 > maxMessages || byteSize + message.length() + 1 > maxBytes)) {
        this.full.complete(null);
        return Optional.empty();
      }
      size += 1;
      byteSize += message.length() + 1;
      try {
        writer.write(message + "\n");
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return Optional.of(result);
    }
  }
}
