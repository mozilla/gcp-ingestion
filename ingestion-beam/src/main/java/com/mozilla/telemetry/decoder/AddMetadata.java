/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.utils.Json;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

public class AddMetadata extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {
  private static final byte[] METADATA_PREFIX = "{\"metadata\":".getBytes();

  @Override
  protected PubsubMessage processElement(PubsubMessage element) throws IOException {
    // Get payload
    final byte[] payload = element.getPayload();
    // Get attributes as bytes, throws IOException
    final byte[] metadata = Json.asBytes(element.getAttributeMap());
    // Ensure that we have a json object with no leading whitespace
    if (payload.length < 2 || payload[0] != '{') {
      throw new IOException("invalid json object: must start with {");
    }
    // Create an output stream for joining metadata with payload
    final ByteArrayOutputStream payloadWithMetadata =
        new ByteArrayOutputStream(METADATA_PREFIX.length + metadata.length + payload.length);
    // Write metadata prefix
    payloadWithMetadata.write(METADATA_PREFIX);
    // Write metadata
    payloadWithMetadata.write(metadata);
    // Start next json field, unless object was empty
    if (payload.length > 2) {
      // Write comma to start the next field
      payloadWithMetadata.write(',');
    }
    // Write payload without leading `{`
    payloadWithMetadata.write(payload, 1, payload.length - 1);
    return new PubsubMessage(payloadWithMetadata.toByteArray(), element.getAttributeMap());
  }
}
