/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.io;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

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

    private final com.google.cloud.bigquery.BigQuery bigquery;
    private final int maxBytes = 10_000_000; // HTTP request size limit: 10 MB
    private final int maxMessages = 10_000; // Maximum rows per request: 10,000
    private final long maxDelayMillis = 1000; // Default to 1 second

    @VisibleForTesting
    final ConcurrentMap<TableId, Batch> batches = new ConcurrentHashMap<>();

    public Write(com.google.cloud.bigquery.BigQuery bigquery) {
      this.bigquery = bigquery;
    }

    @Override
    public CompletableFuture<TableRow> apply(TableRow row) {
      AtomicReference<CompletableFuture<TableRow>> futureRef = new AtomicReference<>();
      batches.compute(row.tableId, (tableId, batch) -> {
        if (batch != null) {
          batch.add(row).ifPresent(futureRef::set);
        }
        if (futureRef.get() == null) {
          batch = new Batch(bigquery, row.tableId);
          batch.add(row).ifPresent(futureRef::set);
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

      private final com.google.cloud.bigquery.BigQuery bigquery;
      private final CompletableFuture<Void> full;
      private final CompletableFuture<InsertAllResponse> result;

      @VisibleForTesting
      final InsertAllRequest.Builder builder;

      private int size = 0;
      private int byteSize = 0;

      private Batch(com.google.cloud.bigquery.BigQuery bigquery, TableId tableId) {
        this.bigquery = bigquery;
        builder = InsertAllRequest.newBuilder(tableId)
            // ignore row values for columns not present in the table
            .setIgnoreUnknownValues(true)
            // insert all valid rows when invalid rows are present in the request
            .setSkipInvalidRows(true);
        full = CompletableFuture.runAsync(this::timeout);
        result = full.handleAsync(this::insertAll);
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
          if (!errors.isEmpty()) {
            throw new WriteErrors(errors);
          }
          return row;
        }));
      }
    }
  }
}
