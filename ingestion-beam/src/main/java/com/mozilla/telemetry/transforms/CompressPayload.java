/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class CompressPayload
    extends PTransform<PCollection<? extends PubsubMessage>, PCollection<PubsubMessage>> {

  public static CompressPayload of(ValueProvider<Compression> compression) {
    return new CompressPayload(compression);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<? extends PubsubMessage> input) {
    return input.apply(
        MapElements.into(new TypeDescriptor<PubsubMessage>(PubsubMessageWithAttributesCoder.class) {
        }).via(message -> compress(message, compression.get())));
  }

  ////////

  private final ValueProvider<Compression> compression;

  private CompressPayload(ValueProvider<Compression> compression) {
    this.compression = compression;
  }

  @VisibleForTesting
  static PubsubMessage compress(PubsubMessage message, Compression compression) {
    byte[] bytes = message.getPayload();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // We use a try-with-resources statement to ensure everything gets closed appropriately.
    try (ReadableByteChannel inChannel = Channels.newChannel(new ByteArrayInputStream(bytes));
        WritableByteChannel outChannel = compression.writeCompressed(Channels.newChannel(out))) {
      ByteStreams.copy(inChannel, outChannel);
    } catch (IOException e) {
      return message;
    }
    return new PubsubMessage(out.toByteArray(), message.getAttributeMap());
  }

}
