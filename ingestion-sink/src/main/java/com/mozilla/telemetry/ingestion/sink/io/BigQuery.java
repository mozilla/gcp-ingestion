package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.util.Json;
import com.mozilla.telemetry.ingestion.sink.config.SinkConfig;
import com.mozilla.telemetry.ingestion.sink.transform.BlobIdToString;
import com.mozilla.telemetry.ingestion.sink.transform.BlobInfoToPubsubMessage;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import com.mozilla.telemetry.ingestion.sink.util.BatchWrite;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BigQuery {

  private BigQuery() {
  }

  public static class BigQueryErrors extends RuntimeException {

    public final List<BigQueryError> errors;

    private BigQueryErrors(List<BigQueryError> errors) {
      super(errors.toString());
      this.errors = errors;
    }
  }

  private static TableId getTableId(String input) {
    final String[] tableSpecParts = input.replaceAll(":", ".").split("\\.", 3);
    if (tableSpecParts.length == 3) {
      return TableId.of(tableSpecParts[0], tableSpecParts[1], tableSpecParts[2]);
    } else if (tableSpecParts.length == 2) {
      return TableId.of(tableSpecParts[0], tableSpecParts[1]);
    } else {
      throw new IllegalArgumentException("TableId requires dataset but none found in: " + input);
    }
  }

  public static class Write
      extends BatchWrite<PubsubMessage, PubsubMessage, TableId, InsertAllResponse> {

    private final com.google.cloud.bigquery.BigQuery bigQuery;
    private final PubsubMessageToObjectNode encoder;

    /** Constructor. */
    public Write(com.google.cloud.bigquery.BigQuery bigQuery, long maxBytes, int maxMessages,
        Duration maxDelay, PubsubMessageToTemplatedString batchKeyTemplate, Executor executor,
        PubsubMessageToObjectNode encoder) {
      super(maxBytes, maxMessages, maxDelay, batchKeyTemplate, executor);
      this.bigQuery = bigQuery;
      this.encoder = encoder;
    }

    @Override
    protected TableId getBatchKey(PubsubMessage input) {
      return getTableId(batchKeyTemplate.apply(input));
    }

    @Override
    protected PubsubMessage encodeInput(PubsubMessage input) {
      return input;
    }

    @Override
    protected Batch getBatch(TableId tableId) {
      return new Batch(tableId);
    }

    @VisibleForTesting
    class Batch extends BatchWrite<PubsubMessage, PubsubMessage, TableId, InsertAllResponse>.Batch {

      @VisibleForTesting
      final InsertAllRequest.Builder builder;

      private Batch(TableId tableId) {
        super();
        builder = InsertAllRequest.newBuilder(tableId)
            // ignore row values for columns not present in the table
            .setIgnoreUnknownValues(true)
            // insert all valid rows when invalid rows are present in the request
            .setSkipInvalidRows(true);
      }

      @Override
      protected CompletableFuture<InsertAllResponse> close() {
        return CompletableFuture.completedFuture(bigQuery.insertAll(builder.build()));
      }

      @Override
      protected void write(PubsubMessage input) {
        Map<String, Object> content = Json.asMap(encoder.apply(input));
        builder.addRow(content);
      }

      @Override
      protected long getByteSize(PubsubMessage input) {
        // plus one for a comma between messages in the HTTP request
        return input.getSerializedSize() + 1;
      }

      @Override
      protected void checkResultFor(InsertAllResponse batchResult, int index) {
        Optional.ofNullable(batchResult.getErrorsFor(index)).filter(errors -> !errors.isEmpty())
            .ifPresent(errors -> {
              throw new BigQueryErrors(errors);
            });
      }
    }
  }

  /**
   * Batch Load GCS blobs to BigQuery, then delete.
   *
   * <p>Optionally delete blobs when load fails, to support optionally batching files from multiple
   * instances via PubSub.
   *
   * <p>GCS blobs should have a 7-day expiration policy if {@code Delete.onSuccess} is specified,
   * so that failed loads are deleted after they will no longer be retried.
   */
  public static class Load extends BatchWrite<PubsubMessage, PubsubMessage, TableId, Void> {

    public enum Delete {
      always, onSuccess
    }

    private final com.google.cloud.bigquery.BigQuery bigQuery;
    private final Storage storage;
    private final Delete delete;
    private static final Pattern OUTPUT_TABLE_PATTERN = Pattern
        .compile("(?:.*/)?" + SinkConfig.OUTPUT_TABLE + "=([^/]+)/.*");

    /** Constructor. */
    public Load(com.google.cloud.bigquery.BigQuery bigQuery, Storage storage, long maxBytes,
        int maxFiles, Duration maxDelay, Executor executor, Delete delete) {
      super(maxBytes, maxFiles, maxDelay, null, executor);
      this.bigQuery = bigQuery;
      this.storage = storage;
      this.delete = delete;
    }

    @Override
    protected TableId getBatchKey(PubsubMessage input) {
      String sourceUri = input.getAttributesOrThrow(BlobInfoToPubsubMessage.NAME);
      final Matcher outputTableMatcher = OUTPUT_TABLE_PATTERN.matcher(sourceUri);
      if (!outputTableMatcher.matches()) {
        throw new IllegalArgumentException(
            String.format("Source URI must match \"%s\" but got \"%s\"",
                OUTPUT_TABLE_PATTERN.pattern(), sourceUri));
      }
      return getTableId(outputTableMatcher.group(1));
    }

    @Override
    protected PubsubMessage encodeInput(PubsubMessage input) {
      return input;
    }

    @Override
    protected Batch getBatch(TableId tableId) {
      return new Batch(tableId);
    }

    @VisibleForTesting
    class Batch extends BatchWrite<PubsubMessage, PubsubMessage, TableId, Void>.Batch {

      @VisibleForTesting
      final List<BlobId> sourceBlobIds = new LinkedList<>();

      @VisibleForTesting
      final TableId tableId;

      private Batch(TableId tableId) {
        super();
        this.tableId = tableId;
      }

      //
      @Override
      protected CompletableFuture<Void> close() {
        List<String> sourceUris = sourceBlobIds.stream().map(BlobIdToString::apply)
            .collect(Collectors.toList());
        try {
          JobStatus status = bigQuery
              .create(JobInfo.of(LoadJobConfiguration.newBuilder(tableId, sourceUris)
                  .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
                  .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                  .setFormatOptions(FormatOptions.json()).setIgnoreUnknownValues(true)
                  .setAutodetect(false).setMaxBadRecords(0).build()))
              .waitFor().getStatus();
          if (status.getError() != null) {
            throw new BigQueryErrors(ImmutableList.of(status.getError()));
          } else if (status.getExecutionErrors() != null
              && status.getExecutionErrors().size() > 0) {
            throw new BigQueryErrors(status.getExecutionErrors());
          }
          if (delete == Delete.onSuccess) {
            delete();
          }
          return CompletableFuture.completedFuture(null);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          if (delete == Delete.always) {
            delete();
          }
        }
      }

      private void delete() {
        try {
          storage.delete(sourceBlobIds);
        } catch (RuntimeException ignore) {
          // don't fail batch when delete throws
        }
      }

      @Override
      protected void write(PubsubMessage input) {
        BlobId blobId = BlobId.of(input.getAttributesOrThrow(BlobInfoToPubsubMessage.BUCKET),
            input.getAttributesOrThrow(BlobInfoToPubsubMessage.NAME));
        if (storage.get(blobId) == null) {
          throw new IllegalArgumentException(
              "blob not found: gs://" + blobId.getBucket() + "/" + blobId.getName());
        }
        sourceBlobIds.add(blobId);
      }

      @Override
      protected long getByteSize(PubsubMessage input) {
        // use blob size
        return Integer.parseInt(input.getAttributesOrThrow(BlobInfoToPubsubMessage.SIZE));
      }
    }
  }
}
