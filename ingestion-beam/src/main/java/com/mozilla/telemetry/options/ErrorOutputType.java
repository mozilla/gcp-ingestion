/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.mozilla.telemetry.transforms.Println;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Enumeration of error output types that each provide a {@code writeFailures} method.
 */
public enum ErrorOutputType {
  stdout {

    /** Return a PTransform that prints errors to STDOUT; only for local running. */
    public PTransform<PCollection<PubsubMessage>, ? extends POutput> writeFailures(
        SinkOptions.Parsed options) {
      return OutputType.print(FORMAT, Println.stdout());
    }
  },

  stderr {

    /** Return a PTransform that prints errors to STDERR; only for local running. */
    public PTransform<PCollection<PubsubMessage>, ? extends POutput> writeFailures(
        SinkOptions.Parsed options) {
      return OutputType.print(FORMAT, Println.stderr());
    }
  },

  file {

    /** Return a PTransform that writes errors to local or remote files. */
    public PTransform<PCollection<PubsubMessage>, ? extends POutput> writeFailures(
        SinkOptions.Parsed options) {
      return OutputType.writeFile(options.getErrorOutput(), FORMAT,
          options.getParsedWindowDuration(), options.getErrorOutputNumShards(),
          options.getErrorOutputFileCompression());
    }
  },

  pubsub {

    /** Return a PTransform that writes to Google Pubsub. */
    public PTransform<PCollection<PubsubMessage>, ? extends POutput> writeFailures(
        SinkOptions.Parsed options) {
      return OutputType.writePubsub(options.getErrorOutput(),
          options.getErrorOutputPubsubCompression());
    }
  };

  public static OutputFileFormat FORMAT = OutputFileFormat.json;

  protected abstract PTransform<PCollection<PubsubMessage>, ? extends POutput> writeFailures(
      SinkOptions.Parsed options);

  /**
   * Return a PTransform that writes to the destination configured in {@code options}.
   */
  public PTransform<PCollection<PubsubMessage>, ? extends POutput> write(
      SinkOptions.Parsed options) {
    if (options.getIncludeStackTrace()) {
      // No transformation to do for the failure messages.
      return writeFailures(options);
    } else {
      // Remove stack_trace attributes before applying writeFailures()
      return PTransform.compose(input -> input.apply("remove stack_trace attributes",
          MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via((PubsubMessage message) -> {
            Map<String, String> filtered = message.getAttributeMap().entrySet().stream() //
                .filter(e -> !e.getKey().startsWith("stack_trace")) //
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
            return new PubsubMessage(message.getPayload(), filtered);
          })).setCoder(PubsubMessageWithAttributesCoder.of()).apply(writeFailures(options)));
    }
  }
}
