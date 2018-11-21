/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

public enum OutputFileFormat {

  text {

    /** Return this PubsubMessage payload as text. */
    public String encodeSingleMessage(PubsubMessage message) {
      if (message == null || message.getPayload() == null) {
        return "";
      }
      return new String(message.getPayload());
    }

    /** Return the appropriate file format suffix for arbitrary text. */
    public String suffix() {
      return ".txt";
    }
  },

  json {

    /** Return this PubsubMessage encoded as a JSON string. */
    public String encodeSingleMessage(PubsubMessage message) {
      try {
        return Json.asString(message);
      } catch (IOException e) {
        // We rely on PubsubMessage construction to fail on invalid data, so we should never
        // see a non-encodable PubsubMessage here and we let the exception bubble up if it happens.
        throw new UncheckedIOException(e);
      }
    }

    /** Return the appropriate file format suffix for newline-delimited JSON. */
    public String suffix() {
      return ".ndjson";
    }
  };

  /** Return a PTransform that encodes PubsubMessages to String. */
  public MapElements<PubsubMessage, String> encode() {
    return MapElements.into(TypeDescriptors.strings()).via(this::encodeSingleMessage);
  }

  public abstract String encodeSingleMessage(PubsubMessage message);

  public abstract String suffix();
}
