/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.util.DurationUtils;
import com.mozilla.telemetry.Sink;
import com.mozilla.telemetry.transforms.CompositeTransform;
import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import com.mozilla.telemetry.transforms.Foreach;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Duration;

/**
 * Enumeration of output types that each provide a {@code write} method.
 *
 * <p>For most outputs, {@code write} is a terminal operation that could return PDone,
 * but we instead return a PCollection that potentially contains error messages,
 * as is the case with BigQuery where we could have failed inserts.
 */
public enum OutputType {
  stdout {
    /** Return a PTransform that prints messages to STDOUT; only for local running. */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        Sink.Options options
    ) {
      return write(options.getOutputFileFormat());
    }

    protected PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        OutputFileFormat format
    ) {
      return CompositeTransform.of((PCollection<PubsubMessage> input) -> {
        input
            .apply(format.encode())
            .apply(Foreach.string(System.out::println));
        return input.apply(NO_ERRORS);
      });
    }
  },

  stderr {
    /** Return a PTransform that prints messages to STDERR; only for local running. */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        Sink.Options options
    ) {
      return write(options.getOutputFileFormat());
    }

    protected PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        OutputFileFormat format
    ) {
      return CompositeTransform.of((PCollection<PubsubMessage> input) -> { input
          .apply(format.encode())
          .apply(Foreach.string(System.err::println));
        return input.apply(NO_ERRORS);
      });
    }
  },

  file {
    /** Return a PTransform that writes to local or remote files. */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        Sink.Options options
    ) {
      Duration windowDuration = parseWindowDuration(options);
      return write(options.getOutput(), options.getOutputFileFormat(), windowDuration);
    }

    protected PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        ValueProvider<String> outputPrefix,
        OutputFileFormat format,
        Duration windowDuration
    ) {
      return CompositeTransform.of((PCollection<PubsubMessage> input) -> {
        input
            .apply(format.encode())
            .apply(Window.into(parseWindow(windowDuration)))
            .apply(TextIO.write().to(outputPrefix).withWindowedWrites());
        return input.apply(NO_ERRORS);
      });
    }
  },

  pubsub {
    /** Return a PTransform that writes to Google Pubsub. */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        Sink.Options options
    ) {
      return write(options.getOutput());
    }

    protected PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        ValueProvider<String> location
    ) {
      return CompositeTransform.of((PCollection<PubsubMessage> input) -> {
        input.apply(PubsubIO.writeMessages().to(location));
        return input.apply(NO_ERRORS);
      });
    }
  },

  bigquery {
    /**
     * Return a PTransform that writes to a BigQuery table;
     * also writes failed BigQuery inserts to the configured error output.
     */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        Sink.Options options
    ) {
      return write(options.getOutput());
    }

    protected PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        ValueProvider<String> tableSpec
    ) {
      return CompositeTransform.of((PCollection<PubsubMessage> input) -> {
        PubsubMessageToTableRow decodeTableRow = new PubsubMessageToTableRow();
        AsJsons<TableRow> encodeTableRow = AsJsons.of(TableRow.class);
        PCollectionTuple tableRows = input.apply(decodeTableRow);
        final WriteResult writeResult = tableRows
            .get(decodeTableRow.mainTag)
            .apply(BigQueryIO
                .writeTableRows()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .to(tableSpec));
        return PCollectionList
            .of(tableRows
                .get(decodeTableRow.errorTag))
            .and(writeResult.getFailedInserts()
                .apply(encodeTableRow)
                .apply(InputFileFormat.text.decode()) // TODO: add error_{type,message} fields
                .get(DecodePubsubMessages.mainTag))
            .apply(Flatten.pCollections());
      });
    }
  };

  public abstract PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
      Sink.Options options
  );

  private static PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> NO_ERRORS =
      CompositeTransform.of(input -> input
          .getPipeline()
          .apply(Create.empty(PubsubMessageWithAttributesCoder.of())
      ));

  public static FixedWindows parseWindow(Duration duration) {
    return FixedWindows.of(duration);
  }

  public static FixedWindows parseWindow(Sink.Options options) {
    return FixedWindows.of(parseWindowDuration(options));
  }

  public static Duration parseWindowDuration(Sink.Options options) {
    return DurationUtils.parseDuration(options.getWindowDuration());
  }

}
