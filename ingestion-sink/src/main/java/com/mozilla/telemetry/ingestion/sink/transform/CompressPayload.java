package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

public enum CompressPayload implements Function<PubsubMessage, PubsubMessage> {
  GZIP {

    /**
     * Perform gzip compression on message data.
     */
    @Override
    public PubsubMessage apply(PubsubMessage message) {
      final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
      final ByteString data;
      // We use a try-with-resources statement to ensure everything gets closed appropriately.
      try {
        GZIPOutputStream gzip = new GZIPOutputStream(compressed);
        message.getData().writeTo(gzip);
        gzip.close();
        data = ByteString.copyFrom(compressed.toByteArray());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return PubsubMessage.newBuilder().putAllAttributes(message.getAttributesMap()).setData(data)
          .build();
    }
  },
  NONE {

    /**
     * Pass all messages through unchanged.
     */
    @Override
    public PubsubMessage apply(PubsubMessage message) {
      return message;
    }
  }
}
