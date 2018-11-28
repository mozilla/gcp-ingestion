/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.ResultWithErrors;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public enum InputType {

  pubsub {

    /** Return a PTransform that reads from a Pubsub subscription. */
    public PTransform<PBegin, ResultWithErrors<PCollection<PubsubMessage>>> read(
        SinkOptions.Parsed options) {
      return PTransform.compose(input -> input
          .apply(PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInput()))
          .apply(MapElementsWithErrors.ToPubsubMessageFrom.identity()));
    }
  },

  file {

    /** Return a PTransform that reads from local or remote files. */
    public PTransform<PBegin, ResultWithErrors<PCollection<PubsubMessage>>> read(
        SinkOptions.Parsed options) {
      return PTransform.compose(input -> input.apply(TextIO.read().from(options.getInput()))
          .apply(options.getInputFileFormat().decode()));
    }
  };

  public abstract PTransform<PBegin, ResultWithErrors<PCollection<PubsubMessage>>> read(
      SinkOptions.Parsed options);
}
