/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.mozilla.telemetry.Sink;
import com.mozilla.telemetry.transforms.CompositeTransform;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;

public enum InputType {

  pubsub {
    /** Return a PTransform that reads from a Pubsub subscription. */
    public PTransform<PBegin, PCollectionTuple> read(Sink.Options options) {
      return CompositeTransform.of(input -> input
          .apply(PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInput()))
          .apply(new MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage>(){})
      );
    }
  },

  file {
    /** Return a PTransform that reads from local or remote files. */
    public PTransform<PBegin, PCollectionTuple> read(Sink.Options options) {
      return CompositeTransform.of(input -> input
          .apply(TextIO.read().from(options.getInput()))
          .apply(options.getInputFileFormat().decode())
      );
    }
  };

  public abstract PTransform<PBegin, PCollectionTuple> read(Sink.Options options);
}
