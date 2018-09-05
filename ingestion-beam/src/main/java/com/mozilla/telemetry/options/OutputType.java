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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

public enum OutputType {
  stdout {
    /** Return a PTransform that prints messages to STDOUT; only for local running. */
    public PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options) {
      return CompositeTransform.of(input -> input
          .apply(options.getOutputFileFormat().encode())
          .apply(Foreach.string(System.out::println))
      );
    }
  },

  stderr {
    /** Return a PTransform that prints messages to STDERR; only for local running. */
    public PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options) {
      return CompositeTransform.of(input -> input
          .apply(options.getOutputFileFormat().encode())
          .apply(Foreach.string(System.err::println)));
    }
  },

  file {
    /** Return a PTransform that writes to local or remote files. */
    public PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options) {
      return CompositeTransform.of(input -> input
          .apply(options.getOutputFileFormat().encode())
          .apply(Window.into(parseWindow(options)))
          .apply(TextIO.write().to(options.getOutput()).withWindowedWrites())
      );
    }
  },

  pubsub {
    /** Return a PTransform that writes to Google Pubsub. */
    public PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options) {
      return PubsubIO.writeMessages().to(options.getOutput());
    }
  },

  bigquery {
    /**
     * Return a PTransform that writes to a BigQuery table;
     * also writes failed BigQuery inserts to the configured error output.
     */
    public PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options) {
      return CompositeTransform.of((PCollection<PubsubMessage> input) -> {
        PubsubMessageToTableRow decodeTableRow = new PubsubMessageToTableRow();
        AsJsons<TableRow> encodeTableRow = AsJsons.of(TableRow.class);
        PCollectionTuple result = input.apply(decodeTableRow);
        result.get(decodeTableRow.errorTag).apply(options.getErrorOutputType().write(options));
        final WriteResult writeResult = result
            .get(decodeTableRow.mainTag)
            .apply(BigQueryIO
                .writeTableRows()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .to(options.getOutput()));
        writeResult.getFailedInserts()
            .apply(encodeTableRow)
            .apply(InputFileFormat.text.decode()) // TODO: add error_{type,message} fields
            .get(DecodePubsubMessages.mainTag)
            .apply(options.getErrorOutputType().write(options));
        return PDone.in(input.getPipeline());
      });
    }
  };

  public abstract PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options);

  public static FixedWindows parseWindow(Sink.Options options) {
    return FixedWindows.of(parseWindowDuration(options));
  }

  public static Duration parseWindowDuration(Sink.Options options) {
    return DurationUtils.parseDuration(options.getWindowDuration());
  }

}
