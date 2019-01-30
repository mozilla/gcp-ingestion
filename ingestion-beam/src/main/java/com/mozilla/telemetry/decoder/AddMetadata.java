/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * A {@code PTransform} that adds the message's attributes into the JSON payload as a top-level
 * "metadata" object.
 *
 * <p>This transform must come after {@code ParsePayload} to ensure any existing
 * "metadata" key in the payload has been removed. Otherwise, this transform could add a
 * duplicate key leading to invalid JSON.
 */
public class AddMetadata extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  public static AddMetadata of() {
    return INSTANCE;
  }

  @Override
  protected PubsubMessage processElement(PubsubMessage message) throws IOException {
    message = PubsubConstraints.ensureNonNull(message);
    // Get payload
    final byte[] payload = message.getPayload();
    // Get attributes as bytes, throws IOException
    final byte[] metadata = Json.asBytes(message.getAttributeMap());
    // Ensure that we have a json object with no leading whitespace
    if (payload.length < 2 || payload[0] != '{') {
      throw new IOException("invalid json object: must start with {");
    }
    // Create an output stream for joining metadata with payload
    final ByteArrayOutputStream payloadWithMetadata = new ByteArrayOutputStream(
        METADATA_PREFIX.length + metadata.length + payload.length);
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
    return new PubsubMessage(payloadWithMetadata.toByteArray(), message.getAttributeMap());
  }

  ////////

  private static final AddMetadata INSTANCE = new AddMetadata();
  private static final byte[] METADATA_PREFIX = "{\"metadata\":".getBytes();

  private AddMetadata() {
  }

}
