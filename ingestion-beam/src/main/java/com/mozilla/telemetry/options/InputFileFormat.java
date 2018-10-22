/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

public enum InputFileFormat {

  text {

    /** Return a PTransform for decoding attribute-free PubsubMessages from payload strings. */
    public PTransform<PCollection<? extends String>, PCollectionTuple> decode() {
      return DecodePubsubMessages.text();
    }
  },

  json {

    /** Return a PTransform for decoding PubsubMessages from JSON strings. */
    public PTransform<PCollection<? extends String>, PCollectionTuple> decode() {
      return DecodePubsubMessages.json();
    }
  };

  public abstract PTransform<PCollection<? extends String>, PCollectionTuple> decode();
}
