/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.mozilla.telemetry.io.Write;
import com.mozilla.telemetry.io.Write.FileOutput;
import com.mozilla.telemetry.io.Write.PrintOutput;
import com.mozilla.telemetry.io.Write.PubsubOutput;
import com.mozilla.telemetry.transforms.Println;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.transforms.WithErrors.Result;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Enumeration of error output types that each provide a {@code writeFailures} method.
 */
public enum ErrorOutputType {
  stdout {

    /** Return a PTransform that prints errors to STDOUT; only for local running. */
    public Write writeFailures(SinkOptions.Parsed options) {
      return new PrintOutput(FORMAT, Println.stdout());
    }
  },

  stderr {

    /** Return a PTransform that prints errors to STDERR; only for local running. */
    public Write writeFailures(SinkOptions.Parsed options) {
      return new PrintOutput(FORMAT, Println.stderr());
    }
  },

  file {

    /** Return a PTransform that writes errors to local or remote files. */
    public Write writeFailures(SinkOptions.Parsed options) {
      return new FileOutput(options.getErrorOutput(), FORMAT, options.getParsedWindowDuration(),
          options.getErrorOutputNumShards(), options.getErrorOutputFileCompression(),
          options.getInputType());
    }
  },

  pubsub {

    /** Return a PTransform that writes to Google Pubsub. */
    public Write writeFailures(SinkOptions.Parsed options) {
      return new PubsubOutput(options.getErrorOutput(), options.getErrorOutputPubsubCompression(),
          PubsubConstraints.MAX_ENCODABLE_MESSAGE_BYTES);
    }
  };

  public static OutputFileFormat FORMAT = OutputFileFormat.json;

  /**
   * Each case in the enum must implement this method to define how to write out messages.
   *
   * @return A PCollection of failure messages about data that could not be written
   */
  protected abstract Write writeFailures(SinkOptions.Parsed options);

  /**
   * Return a PTransform that writes to the destination configured in {@code options}.
   */
  public Write write(SinkOptions.Parsed options) {
    if (options.getIncludeStackTrace()) {
      // No transformation to do for the failure messages.
      return writeFailures(options);
    } else {
      // Remove stack_trace attributes before applying writeFailures()
      return new Write() {

        @Override
        public Result<PDone> expand(PCollection<PubsubMessage> input) {
          return input
              .apply("remove stack_trace attributes",
                  MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                      .via(message -> new PubsubMessage(message.getPayload(),
                          message.getAttributeMap().entrySet().stream()
                              .filter(e -> !e.getKey().startsWith("stack_trace"))
                              .collect(Collectors.toMap(Entry::getKey, Entry::getValue)))))
              .apply(writeFailures(options));
        }
      };
    }
  }
}
