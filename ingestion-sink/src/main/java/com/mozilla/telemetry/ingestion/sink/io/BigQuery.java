/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuery {

  private BigQuery() {
  }

  @VisibleForTesting
  static class WriteErrors extends RuntimeException {

    public final List<BigQueryError> errors;

    private WriteErrors(List<BigQueryError> errors) {
      this.errors = errors;
    }
  }

  public static class Write implements Function<Write.TableRow, CompletableFuture<Write.TableRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(Write.class);

    private final com.google.cloud.bigquery.BigQuery bigquery;
    private final int maxBytes;
    private final int maxMessages;
    private final long maxDelayMillis;

    @VisibleForTesting
    final ConcurrentMap<TableId, Batch> batches = new ConcurrentHashMap<>();

    public Write(com.google.cloud.bigquery.BigQuery bigquery, int maxBytes, int maxMessages,
        Duration maxDelay) {
      this.bigquery = bigquery;
      this.maxBytes = maxBytes;
      this.maxMessages = maxMessages;
      this.maxDelayMillis = maxDelay.toMillis();
    }

    @Override
    public CompletableFuture<TableRow> apply(TableRow row) {
      AtomicReference<CompletableFuture<TableRow>> futureRef = new AtomicReference<>();
      batches.compute(row.tableId, (tableId, batch) -> {
        if (batch != null) {
          batch.add(row).ifPresent(futureRef::set);
        }
        if (futureRef.get() == null) {
          batch = new Batch(bigquery, row);
          batch.first.ifPresent(futureRef::set);
        }
        return batch;
      });
      return Optional.ofNullable(futureRef.get())
          .orElseThrow(() -> new IllegalArgumentException("Empty batch rejected message"));
    }

    public static class TableRow {

      public final TableId tableId;
      public final int byteSize;
      public final Optional<String> id;
      public final Map<String, Object> content;

      public TableRow(TableId tableId, int byteSize, Optional<String> id,
          Map<String, Object> content) {
        this.tableId = tableId;
        this.byteSize = byteSize;
        this.id = id;
        this.content = content;
      }
    }

    @VisibleForTesting
    class Batch {

      final Optional<CompletableFuture<TableRow>> first;

      private final com.google.cloud.bigquery.BigQuery bigquery;
      private final CompletableFuture<Void> full;
      private final CompletableFuture<InsertAllResponse> result;

      @VisibleForTesting
      final InsertAllRequest.Builder builder;

      private int size = 0;
      private int byteSize = 0;

      private Batch(com.google.cloud.bigquery.BigQuery bigquery, TableRow row) {
        this.bigquery = bigquery;
        builder = InsertAllRequest.newBuilder(row.tableId)
            // ignore row values for columns not present in the table
            .setIgnoreUnknownValues(true)
            // insert all valid rows when invalid rows are present in the request
            .setSkipInvalidRows(true);

        // block full from completing before the first add()
        CompletableFuture<Void> init = new CompletableFuture<>();
        full = init.thenRunAsync(this::timeout);
        result = full.handleAsync(this::insertAll);
        // expose the return value for the first add()
        first = add(row);
        // allow full to complete
        init.complete(null);
      }

      private void timeout() {
        try {
          Thread.sleep(maxDelayMillis);
        } catch (InterruptedException e) {
          // this is fine
        }
      }

      private synchronized InsertAllResponse insertAll(Void v, Throwable t) {
        return bigquery.insertAll(builder.build());
      }

      private synchronized Optional<CompletableFuture<TableRow>> add(TableRow row) {
        if (full.isDone()) {
          return Optional.empty();
        }
        int newSize = size + 1;
        // Plus one byte for the comma between messages
        int newByteSize = byteSize + row.byteSize + 1;
        if (newSize > maxMessages || newByteSize > maxBytes) {
          this.full.complete(null);
          return Optional.empty();
        }
        size = newSize;
        byteSize = newByteSize;
        if (row.id.isPresent()) {
          builder.addRow(row.id.get(), row.content);
        } else {
          builder.addRow(row.content);
        }
        int index = newSize - 1;
        return Optional.of(result.thenApplyAsync(r -> {
          List<BigQueryError> errors = r.getErrorsFor(index);
          if (errors != null && !errors.isEmpty()) {
            LOG.warn(errors.toString());
            throw new WriteErrors(errors);
          }
          return row;
        }));
      }
    }
  }
}
