package com.mozilla.telemetry.transforms;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class CompressPayload
    extends PTransform<PCollection<? extends PubsubMessage>, PCollection<PubsubMessage>> {

  public static CompressPayload of(ValueProvider<Compression> compression) {
    return new CompressPayload(compression, Integer.MAX_VALUE);
  }

  public CompressPayload withMaxCompressedBytes(int maxBytes) {
    return new CompressPayload(this.compression, maxBytes);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<? extends PubsubMessage> input) {
    return input
        .apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via(this::compress));
  }

  ////////

  final ValueProvider<Compression> compression;
  private final int maxCompressedBytes;

  private final Counter truncationCounter = Metrics.counter(CompressPayload.class,
      "truncated_payload");

  private CompressPayload(ValueProvider<Compression> compression, int maxCompressedBytes) {
    this.compression = compression;
    this.maxCompressedBytes = maxCompressedBytes;
  }

  @VisibleForTesting
  PubsubMessage compress(PubsubMessage message) {
    message = PubsubConstraints.ensureNonNull(message);
    byte[] compressedBytes = compress(message.getPayload(), compression.get());
    if (compressedBytes.length > maxCompressedBytes) {
      byte[] truncated = Arrays.copyOfRange(message.getPayload(), 0, maxCompressedBytes);
      truncationCounter.inc();
      compressedBytes = compress(truncated, compression.get());
    }
    return new PubsubMessage(compressedBytes, message.getAttributeMap(), message.getMessageId());
  }

  @VisibleForTesting
  static byte[] compress(byte[] payload, Compression compression) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // We use a try-with-resources statement to ensure everything gets closed appropriately.
    try (ReadableByteChannel inChannel = Channels.newChannel(new ByteArrayInputStream(payload));
        WritableByteChannel outChannel = compression.writeCompressed(Channels.newChannel(out))) {
      ByteStreams.copy(inChannel, outChannel);
    } catch (IOException e) {
      return payload;
    }
    return out.toByteArray();
  }

}
