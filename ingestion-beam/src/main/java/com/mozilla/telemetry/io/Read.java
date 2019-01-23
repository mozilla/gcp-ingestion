/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.io;

import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.options.SinkOptions.Parsed;
import com.mozilla.telemetry.transforms.MapElementsWithErrors.ToPubsubMessageFrom;
import com.mozilla.telemetry.transforms.WithErrors;
import com.mozilla.telemetry.transforms.WithErrors.Result;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Implementations of reading from the sources enumerated in {@link
 * com.mozilla.telemetry.options.InputType}.
 */
public abstract class Read
    extends PTransform<PBegin, WithErrors.Result<PCollection<PubsubMessage>>> {

  /** Implementation of reading from Pub/Sub. */
  public static class PubsubInput extends Read {

    private final SinkOptions.Parsed options;

    public PubsubInput(Parsed options) {
      this.options = options;
    }

    @Override
    public Result<PCollection<PubsubMessage>> expand(PBegin input) {
      return input //
          .apply(PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInput()))
          .apply(ToPubsubMessageFrom.identity());
    }
  }

  /** Implementation of reading from local or remote files. */
  public static class FileInput extends Read {

    private final SinkOptions.Parsed options;

    public FileInput(Parsed options) {
      this.options = options;
    }

    @Override
    public Result<PCollection<PubsubMessage>> expand(PBegin input) {
      return input //
          .apply(TextIO.read().from(options.getInput())) //
          .apply(options.getInputFileFormat().decode());
    }
  }
}
