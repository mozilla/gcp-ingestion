package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.util.Json;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import com.mozilla.telemetry.ingestion.sink.util.BatchWrite;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;

public class Gcs {

  private Gcs() {
  }

  public abstract static class Write extends BatchWrite<PubsubMessage, byte[], String, Void> {

    public static class Ndjson extends Write {

      private final PubsubMessageToObjectNode encoder;

      public Ndjson(Storage storage, long maxBytes, int maxMessages, Duration maxDelay,
          PubsubMessageToTemplatedString batchKeyTemplate, PubsubMessageToObjectNode encoder,
          Function<BlobInfo, CompletableFuture<Void>> batchCloseHook) {
        super(storage, maxBytes, maxMessages, maxDelay, batchKeyTemplate, batchCloseHook);
        this.encoder = encoder;
      }

      @Override
      protected byte[] encodeInput(PubsubMessage input) {
        try {
          return ArrayUtils.addAll(Json.asBytes(encoder.apply(input)),
              "\n".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    private final Storage storage;
    private final Function<BlobInfo, CompletableFuture<Void>> batchCloseHook;

    private Write(Storage storage, long maxBytes, int maxMessages, Duration maxDelay,
        PubsubMessageToTemplatedString batchKeyTemplate,
        Function<BlobInfo, CompletableFuture<Void>> batchCloseHook) {
      super(maxBytes, maxMessages, maxDelay, batchKeyTemplate);
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
      return new Batch(storage, gcsPrefixMatcher.group(BUCKET), gcsPrefixMatcher.group(NAME));
    }

    @VisibleForTesting
    class Batch extends BatchWrite<PubsubMessage, byte[], String, Void>.Batch {

      @VisibleForTesting
      final BlobInfo blobInfo;

      private final WriteChannel writer;

      private Batch(Storage storage, String bucket, String keyPrefix) {
        super();
        // save blobInfo for batchCloseHook
        blobInfo = BlobInfo
            .newBuilder(BlobId.of(bucket, keyPrefix + UUID.randomUUID().toString() + ".ndjson"))
            .setContentType("application/json").build();
        writer = storage.writer(blobInfo);
      }

      @Override
      protected CompletableFuture<Void> close(Void ignore) {
        try {
          writer.close();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        BlobInfo blobInfoWithSize = storage.get(blobInfo.getBlobId());
        return batchCloseHook.apply(blobInfoWithSize);
      }

      @Override
      protected void write(byte[] encodedInput) {
        try {
          writer.write(ByteBuffer.wrap(encodedInput));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      protected long getByteSize(byte[] encodedInput) {
        return encodedInput.length;
      }
    }
  }
}
