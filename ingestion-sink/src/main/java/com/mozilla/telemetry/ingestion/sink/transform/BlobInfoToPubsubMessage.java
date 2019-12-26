package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.cloud.storage.BlobInfo;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.charset.StandardCharsets;

public class BlobInfoToPubsubMessage {

  /**
   * Read the contents of a GCS blob into a {@link PubsubMessage}.
   */
  public static PubsubMessage apply(BlobInfo blobInfo) {
    return PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom(("gs://" + blobInfo.getBucket() + "/" + blobInfo.getName())
            .getBytes(StandardCharsets.UTF_8)))
        .putAttributes("size", blobInfo.getSize().toString()).build();
  }
}
