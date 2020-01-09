package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.cloud.storage.BlobId;

public class BlobIdToString {

  public static String apply(BlobId blobId) {
    return "gs://" + blobId.getBucket() + "/" + blobId.getName();
  }
}
