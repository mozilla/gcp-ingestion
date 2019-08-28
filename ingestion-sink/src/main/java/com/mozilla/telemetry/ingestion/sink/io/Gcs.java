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
import java.time.Duration;
import java.util.UUID;

public class Gcs {

  private Gcs() {
  }

  public abstract static class Write extends BatchWrite<PubsubMessage, byte[], String, BlobInfo> {

    public static class Ndjson extends Write {

      private final PubsubMessageToJSONObject encoder;

      public Ndjson(Storage storage, long maxBytes, int maxMessages, Duration maxDelay,
          String batchKeyTemplate, Format format) {
        super(storage, maxBytes, maxMessages, maxDelay, batchKeyTemplate);
        this.encoder = new PubsubMessageToJSONObject(format);
      }

      @Override
      protected byte[] encodeInput(PubsubMessage input) {
        return (encoder.apply(input).toString() + "\n").getBytes();
      }
    }

    private final Storage storage;

    private Write(Storage storage, long maxBytes, int maxMessages, Duration maxDelay,
        String batchKeyTemplate) {
      super(maxBytes, maxMessages, maxDelay, batchKeyTemplate);
      this.storage = storage;
    }

    @Override
    protected String getBatchKey(PubsubMessage input) {
      return batchKeyTemplate.apply(input);
    }

    @Override
    protected Batch getBatch(String gcsPrefix) {
      return new Batch(storage, gcsPrefix);
    }

    @VisibleForTesting
    class Batch extends BatchWrite<PubsubMessage, byte[], String, BlobInfo>.Batch {

      @VisibleForTesting
      final BlobInfo blobInfo;

      private final WriteChannel writer;

      private Batch(Storage storage, String gcsPrefix) {
        super();
        // save blobInfo for the response from this.close
        final String[] outputPrefixParts = gcsPrefix.split("/", 2);
        final String bucket;
        final String keyPrefix;
        if (outputPrefixParts.length == 2) {
          bucket = outputPrefixParts[0];
          keyPrefix = outputPrefixParts[1];
        } else {
          bucket = gcsPrefix;
          keyPrefix = "";
        }
        blobInfo = BlobInfo
            .newBuilder(BlobId.of(bucket, keyPrefix + UUID.randomUUID().toString() + ".ndjson"))
            .setContentType("application/json").build();
        writer = storage.writer(blobInfo);
      }

      @Override
      protected BlobInfo close(Void ignoreVoid, Throwable ignoreThrowable) {
        try {
          writer.close();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return blobInfo;
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
