/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.io;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.mozilla.telemetry.ingestion.util.KV;
import java.nio.file.Files;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

public class Gcs {

  public static class Write implements Function<KV<String, File>, String> {

    private final Storage storage = StorageOptions.getDefaultInstance().getService(); // GCS Service
    private final String bucket; // Destination bucket

    public Write(String bucket) {
      this.bucket = bucket;
    }

    public String apply(KV<String, File> kv) {
      // determine gcs key
      // TODO maybe include InetAddress.getLocalHost().getHostName() and System.currentTimeMillis()
      final String key = kv.key + kv.value.getName() + ".ndjson";
      // specify destination and upload options
      final BlobInfo dest = BlobInfo.newBuilder(BlobId.of(bucket, key))
          .setContentType("application/json").build();
      try {
        // upload
        storage.create(dest, Files.readAllBytes(kv.value.toPath()));
      } catch (IOException e) {
        // re-raise unchecked
        throw new UncheckedIOException(e);
      }
      // return gcs path
      return dest.getName();
    }
  }
}
