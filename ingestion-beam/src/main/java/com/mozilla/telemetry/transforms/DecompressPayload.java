/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.common.primitives.Ints;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
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
  public static DecompressPayload enabled(ValueProvider<Boolean> enabled) {
    return new DecompressPayload(enabled);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }

  ////////

  private final ValueProvider<Boolean> enabled;

  private DecompressPayload(ValueProvider<Boolean> enabled) {
    this.enabled = enabled;
  }

  private class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private static final byte ZERO_BYTE = 0x00;

    private final Counter compressedInput = Metrics.counter(Fn.class, "compressed-input");
    private final Counter uncompressedInput = Metrics.counter(Fn.class, "uncompressed-input");

    @Override
    public PubsubMessage apply(PubsubMessage value) {
      byte[] payload = value.getPayload();
      if (enabled.isAccessible() && !enabled.get()) {
        // Compression has been explicitly turned off in options, so return unchanged message.
        return value;
      } else if (!isGzip(payload)) {
        uncompressedInput.inc();
        return value;
      } else {
        try {
          //
          ByteArrayInputStream payloadStream = new ByteArrayInputStream(value.getPayload());
          GZIPInputStream gzipStream = new GZIPInputStream(payloadStream);
          ByteArrayOutputStream decompressedStream = new ByteArrayOutputStream();
          // Throws IOException
          IOUtils.copy(gzipStream, decompressedStream);
          payload = decompressedStream.toByteArray();
          compressedInput.inc();
        } catch (IOException ignore) {
          // payload wasn't valid gzip, assume it wasn't compressed
          uncompressedInput.inc();
        }
        return new PubsubMessage(payload, value.getAttributeMap());
      }
    }

    /**
     * See implementation of {@link org.apache.beam.sdk.io.Compression#GZIP}.
     */
    private boolean isGzip(byte[] bytes) {
      return bytes.length >= 2
          && Ints.fromBytes(ZERO_BYTE, ZERO_BYTE, bytes[1], bytes[0]) == GZIPInputStream.GZIP_MAGIC;
    }
  }

  private final Fn fn = new Fn();
}
