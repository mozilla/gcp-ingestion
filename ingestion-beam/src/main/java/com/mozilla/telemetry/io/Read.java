/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.io;

import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.transforms.MapElementsWithErrors.ToPubsubMessageFrom;
import com.mozilla.telemetry.transforms.WithErrors;
import com.mozilla.telemetry.transforms.WithErrors.Result;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
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

    private final ValueProvider<String> subscription;

    public PubsubInput(ValueProvider<String> subscription) {
      this.subscription = subscription;
    }

    @Override
    public Result<PCollection<PubsubMessage>> expand(PBegin input) {
      return input //
          .apply(PubsubIO.readMessagesWithAttributes().fromSubscription(subscription))
          .apply(ToPubsubMessageFrom.identity());
    }
  }

  /** Implementation of reading from local or remote files. */
  public static class FileInput extends Read {

    private final ValueProvider<String> fileSpec;
    private final InputFileFormat fileFormat;

    public FileInput(ValueProvider<String> fileSpec, InputFileFormat fileFormat) {
      this.fileSpec = fileSpec;
      this.fileFormat = fileFormat;
    }

    @Override
    public Result<PCollection<PubsubMessage>> expand(PBegin input) {
      return input.apply(TextIO.read().from(fileSpec)).apply(fileFormat.decode());
    }
  }
}
