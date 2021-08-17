package com.mozilla.telemetry.transforms;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.IOUtils;

public class DecompressPayload
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  /**
   * Return a {@link PTransform} that performs gzip decompression on gzipped message payloads and
   * passes other messages through unchanged.
   *
   * @param enabled if false, this transform will pass all messages through unchanged
   */
  public static DecompressPayload enabled(Boolean enabled) {
    return new DecompressPayload(enabled, false);
  }

  public DecompressPayload withClientCompressionRecorded() {
    return new DecompressPayload(enabled, true);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }

  ////////

  private final Boolean enabled;
  private final boolean recordInputCompression;

  private DecompressPayload(Boolean enabled, boolean recordInputCompression) {
    this.enabled = enabled;
    this.recordInputCompression = recordInputCompression;
  }

  private class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private final Counter compressedInput = Metrics.counter(Fn.class, "compressed_input");
    private final Counter uncompressedInput = Metrics.counter(Fn.class, "uncompressed_input");

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      if (!enabled) {
        // Compression has been explicitly turned off in options, so return unchanged message.
        return message;
      } else {
        message = PubsubConstraints.ensureNonNull(message);
        try {
          ByteArrayInputStream payloadStream = new ByteArrayInputStream(message.getPayload());
          GZIPInputStream gzipStream = new GZIPInputStream(payloadStream);
          ByteArrayOutputStream decompressedStream = new ByteArrayOutputStream();
          // Throws IOException
          IOUtils.copy(gzipStream, decompressedStream);
          compressedInput.inc();
          Map<String, String> attributes = message.getAttributeMap();
          if (recordInputCompression) {
            attributes = new HashMap<>(message.getAttributeMap());
            attributes.putIfAbsent(Attribute.CLIENT_COMPRESSION, "gzip");
          }
          return new PubsubMessage(decompressedStream.toByteArray(), attributes);
        } catch (IOException ignore) {
          // payload wasn't valid gzip, assume it wasn't compressed
          uncompressedInput.inc();
          return message;
        }
      }
    }

  }

  private final Fn fn = new Fn();
}
