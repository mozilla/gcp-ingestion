/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.IOUtils;

public class GzipDecompress
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {
  private class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {
    @Override
    public PubsubMessage apply(PubsubMessage value) {
      byte[] payload = value.getPayload();
      try {
        //
        ByteArrayInputStream payloadStream = new ByteArrayInputStream(value.getPayload());
        GZIPInputStream gzipStream = new GZIPInputStream(payloadStream);
        ByteArrayOutputStream decompressedStream = new ByteArrayOutputStream();
        // Throws IOException
        IOUtils.copy(gzipStream, decompressedStream);
        payload = decompressedStream.toByteArray();
      } catch (IOException ignore) {
        // payload wasn't valid gzip, assume it wasn't compressed
      }
      return new PubsubMessage(payload, value.getAttributeMap());
    }
  }

  private final Fn fn = new Fn();

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }
}
