/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.mozilla.telemetry.transforms.PubsubMessageMixin;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

public enum OutputFileFormat {

  text {
    /** Return a PTransform that encodes just the payload of PubsubMessages as text. */
    public MapElements<PubsubMessage, String> encode() {
      return MapElements
          .into(TypeDescriptors.strings())
          .via((PubsubMessage value) -> new String(value.getPayload()));
    }
  },

  json {
    /** Return a PTransform that encodes PubsubMessages as JSON. */
    public MapElements<PubsubMessage, String> encode() {
      return MapElements
          .into(TypeDescriptors.strings())
          .via((PubsubMessage message) -> {
            try {
              return PubsubMessageMixin.MAPPER.writeValueAsString(message);
            } catch (Throwable e) {
              throw new RuntimeException();
            }
          });
    }
  };

  public abstract MapElements<PubsubMessage, String> encode();
}
