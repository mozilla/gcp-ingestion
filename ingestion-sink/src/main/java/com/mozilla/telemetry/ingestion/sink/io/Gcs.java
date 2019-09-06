/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToJSONObject;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToJSONObject.Format;
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

public class Gcs {

  private Gcs() {
  }

  public abstract static class Write extends BatchWrite<PubsubMessage, byte[], String, Void> {

    public static class Ndjson extends Write {

      private final PubsubMessageToJSONObject encoder;

      public Ndjson(Storage storage, long maxBytes, int maxMessages, Duration maxDelay,
          String batchKeyTemplate, Format format,
          Function<BlobInfo, CompletableFuture<Void>> batchCloseHook) {
        super(storage, maxBytes, maxMessages, maxDelay, batchKeyTemplate, batchCloseHook);
        this.encoder = new PubsubMessageToJSONObject(format);
      }

      @Override
      protected byte[] encodeInput(PubsubMessage input) {
        return (encoder.apply(input).toString() + "\n").getBytes(StandardCharsets.UTF_8);
      }
    }

    private final Storage storage;
    private final Function<BlobInfo, CompletableFuture<Void>> batchCloseHook;

    private Write(Storage storage, long maxBytes, int maxMessages, Duration maxDelay,
        String batchKeyTemplate, Function<BlobInfo, CompletableFuture<Void>> batchCloseHook) {
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
    private static final Pattern blobIdPattern = Pattern
        .compile("(gs://)?(?<" + BUCKET + ">[^/]+)(/(?<" + NAME + ">.*))?");

    @Override
    protected Batch getBatch(String gcsPrefix) {
      final Matcher gcsPrefixMatcher = blobIdPattern.matcher(gcsPrefix);
      if (!gcsPrefixMatcher.matches()) {
        throw new IllegalArgumentException(
            String.format("Gcs prefix must match \"%s\" but got \"%s\" from: %s",
                blobIdPattern.pattern(), gcsPrefix, batchKeyTemplate.template));
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
        return batchCloseHook.apply(blobInfo);
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
