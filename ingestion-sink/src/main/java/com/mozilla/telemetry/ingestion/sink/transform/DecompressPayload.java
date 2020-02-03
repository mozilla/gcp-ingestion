package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

public enum DecompressPayload implements Function<PubsubMessage, PubsubMessage> {
  GZIP {

    /**
     * Perform decompression on gzip compressed data and pass other messages through unchanged.
     */
    @Override
    public PubsubMessage apply(PubsubMessage message) {
      final ByteArrayInputStream dataStream = new ByteArrayInputStream(
          message.getData().toByteArray());
      final ByteString decompressed;
      try {
        final GZIPInputStream gzipStream = new GZIPInputStream(dataStream);
        decompressed = ByteString.readFrom(gzipStream);
      } catch (IOException ignore) {
        // payload wasn't valid gzip, assume it wasn't compressed
        return message;
      }
      return PubsubMessage.newBuilder().putAllAttributes(message.getAttributesMap())
          .setData(decompressed).build();
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
