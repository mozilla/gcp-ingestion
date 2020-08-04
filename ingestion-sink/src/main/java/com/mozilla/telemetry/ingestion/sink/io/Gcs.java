package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.core.util.Json;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import com.mozilla.telemetry.ingestion.sink.util.BatchWrite;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;

public class Gcs {

  private Gcs() {
  }

  public abstract static class Write
      extends BatchWrite<PubsubMessage, Write.LazyEncodedInput, String, Void> {

    public static class Ndjson extends Write {

      private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);

      private final PubsubMessageToObjectNode encoder;

      public Ndjson(Storage storage, long maxBytes, int maxMessages, Duration maxDelay,
          PubsubMessageToTemplatedString batchKeyTemplate, Executor executor,
          PubsubMessageToObjectNode encoder,
          Function<Blob, CompletableFuture<Void>> batchCloseHook) {
        super(storage, maxBytes, maxMessages, maxDelay, batchKeyTemplate, executor, batchCloseHook);
        this.encoder = encoder;
      }

      @Override
      protected LazyEncodedInput encodeInput(PubsubMessage input) {
        return new LazyEncodedInput(input);
      }

      private class LazyEncodedInput extends Write.LazyEncodedInput {

        private final PubsubMessage input;

        private LazyEncodedInput(PubsubMessage input) {
          this.input = input;
        }

        @Override
        protected byte[] encode(TableId tableId) {
          return ArrayUtils.addAll(
              Json.asBytes(
                  encoder.apply(tableId, input.getAttributesMap(), input.getData().toByteArray())),
              NEWLINE);
        }
      }
    }

    private final Storage storage;
    private final Function<Blob, CompletableFuture<Void>> batchCloseHook;

    private Write(Storage storage, long maxBytes, int maxMessages, Duration maxDelay,
        PubsubMessageToTemplatedString batchKeyTemplate, Executor executor,
        Function<Blob, CompletableFuture<Void>> batchCloseHook) {
      super(maxBytes, maxMessages, maxDelay, batchKeyTemplate, executor);
      this.storage = storage;
      this.batchCloseHook = batchCloseHook;
    }

    @Override
    protected String getBatchKey(PubsubMessage input) {
      return batchKeyTemplate.apply(input);
    }

    private static final String BUCKET = "bucket";
    private static final String NAME = "name";
    private static final Pattern BLOB_ID_PATTERN = Pattern
        .compile("(gs://)?(?<" + BUCKET + ">[^/]+)(/(?<" + NAME + ">.*))?");

    @Override
    protected Batch getBatch(String gcsPrefix) {
      final Matcher gcsPrefixMatcher = BLOB_ID_PATTERN.matcher(gcsPrefix);
      if (!gcsPrefixMatcher.matches()) {
        throw new IllegalArgumentException(
            String.format("Gcs prefix must match \"%s\" but got \"%s\" from: %s",
                BLOB_ID_PATTERN.pattern(), gcsPrefix, batchKeyTemplate.template));
      }
      return new Batch(gcsPrefixMatcher.group(BUCKET), gcsPrefixMatcher.group(NAME));
    }

    /**
     * Delay encoding until {@link TableId} is available and cache the result.
     */
    private abstract class LazyEncodedInput {

      private byte[] result = null;

      protected abstract byte[] encode(TableId tableId);

      public byte[] get(TableId tableId) {
        if (result == null) {
          result = encode(tableId);
        }
        return result;
      }
    }

    @VisibleForTesting
    class Batch extends BatchWrite<PubsubMessage, LazyEncodedInput, String, Void>.Batch {

      @VisibleForTesting
      final BlobInfo blobInfo;
      private final TableId tableId;

      private final ByteArrayOutputStream content = new ByteArrayOutputStream();

      private Batch(String bucket, String keyPrefix) {
        super();
        tableId = BigQuery.getTableId(keyPrefix);
        // save blobInfo for batchCloseHook
        blobInfo = BlobInfo
            .newBuilder(BlobId.of(bucket, keyPrefix + UUID.randomUUID().toString() + ".ndjson"))
            .setContentType("application/json").build();
      }

      /**
       * Throws 'com.google.cloud.storage.StorageException: Precondition Failed'
       * if blob already exists.
       */
      @Override
      protected CompletableFuture<Void> close() {
        return batchCloseHook.apply(
            storage.create(blobInfo, content.toByteArray(), BlobTargetOption.doesNotExist()));
      }

      @Override
      protected void write(LazyEncodedInput encodedInput) {
        byte[] result = encodedInput.get(tableId);
        content.write(result, 0, result.length);
      }

      @Override
      protected long getByteSize(LazyEncodedInput encodedInput) {
        return encodedInput.get(tableId).length;
      }
    }
  }
}
