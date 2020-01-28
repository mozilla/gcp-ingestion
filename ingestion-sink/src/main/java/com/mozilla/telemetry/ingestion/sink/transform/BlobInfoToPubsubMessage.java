package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.cloud.storage.BlobInfo;
import com.google.pubsub.v1.PubsubMessage;

public class BlobInfoToPubsubMessage {

  public static final String BUCKET = "bucket";
  public static final String NAME = "name";
  public static final String SIZE = "size";

  /**
   * Read the contents of a GCS blob into a {@link PubsubMessage}.
   */
  public static PubsubMessage apply(BlobInfo blobInfo) {
    return PubsubMessage.newBuilder().putAttributes(BUCKET, blobInfo.getBucket())
        .putAttributes(NAME, blobInfo.getName()).putAttributes(SIZE, blobInfo.getSize().toString())
        .build();
  }
}
