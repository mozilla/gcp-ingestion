package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.cloud.storage.BlobId;
import com.google.pubsub.v1.PubsubMessage;

public class BlobIdToPubsubMessage {

  public static final String BUCKET = "bucket";
  public static final String NAME = "name";

  /**
   * Convert contents of a {@link BlobId} into a {@link PubsubMessage}.
   */
  public static PubsubMessage encode(BlobId blobId) {
    return PubsubMessage.newBuilder().putAttributes(BUCKET, blobId.getBucket())
        .putAttributes(NAME, blobId.getName()).build();
  }

  /**
   * Read the contents of a {@link PubsubMessage} into a {@link BlobId}.
   */
  public static BlobId decode(PubsubMessage message) {
    return BlobId.of(message.getAttributesOrThrow(BUCKET), message.getAttributesOrThrow(NAME));
  }
}
