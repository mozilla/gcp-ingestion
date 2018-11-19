/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.google.api.services.bigquery.model.TableRow;
import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import com.mozilla.telemetry.transforms.Println;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow;
import com.mozilla.telemetry.util.DynamicPathTemplate;
import java.util.List;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
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

    /**
     * Return a PTransform that prints messages to STDOUT; only for local running.
     */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        SinkOptions.Parsed options) {
      return PTransform.compose((PCollection<PubsubMessage> input) -> {
        input.apply("print to stdout", print(options.getOutputFileFormat(), Println.stdout()));
        return input.getPipeline().apply("return empty error collection", EMPTY_ERROR_COLLECTION);
      });
    }
  },

  stderr {

    /** Return a PTransform that prints messages to STDERR; only for local running. */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        SinkOptions.Parsed options) {
      return PTransform.compose((PCollection<PubsubMessage> input) -> {
        input.apply("print to stderr", print(options.getOutputFileFormat(), Println.stderr()));
        return input.getPipeline().apply("return empty error collection", EMPTY_ERROR_COLLECTION);
      });
    }
  },

  file {

    /** Return a PTransform that writes to local or remote files. */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        SinkOptions.Parsed options) {
      return PTransform.compose((PCollection<PubsubMessage> input) -> {
        input.apply("write files", writeFile(options.getOutput(), options.getOutputFileFormat(),
            options.getParsedWindowDuration()));
        return input.getPipeline().apply("return empty error collection", EMPTY_ERROR_COLLECTION);
      });
    }
  },

  pubsub {

    /** Return a PTransform that writes to Google Pubsub. */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        SinkOptions.Parsed options) {
      return PTransform.compose((PCollection<PubsubMessage> input) -> {
        input.apply("write to pubsub", writePubsub(options.getOutput()));
        return input.getPipeline().apply("return empty error collection", EMPTY_ERROR_COLLECTION);
      });
    }
  },

  bigquery {

    /** Return a PTransform that writes to a BigQuery table and collects failed inserts. */
    public PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
        SinkOptions.Parsed options) {
      return writeBigQuery(options.getOutput());
    }
  };

  /**
   * Each case in the enum must implement this method to define how to write out messages.
   *
   * @return A PCollection of failure messages about data that could not be written.
   */
  public abstract PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> write(
      SinkOptions.Parsed options);

  /*
   * Static methods giving concrete implementations of writing to various outputs. These are also
   * accessed by ErrorOutputType.
   */

  protected static PTransform<PCollection<PubsubMessage>, PDone> print(OutputFileFormat format,
      PTransform<PCollection<String>, PDone> output) {
    return PTransform.compose(input -> input
        .apply("endcode PubsubMessages as strings", format.encode()).apply("print", output));
  }

  /**
   * For details of the intended behavior for file paths, see:
   * https://github.com/mozilla/gcp-ingestion/tree/master/ingestion-beam#output-path-specification
   */
  protected static PTransform<PCollection<PubsubMessage>, POutput> writeFile(
      ValueProvider<String> outputPrefix, OutputFileFormat format, Duration windowDuration) {
    DynamicPathTemplate pathTemplate = new DynamicPathTemplate(outputPrefix.get());
    return PTransform
        .compose(input -> input.apply("fixedWindows", Window.into(FixedWindows.of(windowDuration)))
            .apply("writeFile", FileIO.<List<String>, PubsubMessage>writeDynamic()
                // We can't pass the attribute map to by() directly since MapCoder isn't
                // deterministic;
                // instead, we extract an ordered list of the needed placeholder values.
                // That list is later available to withNaming() to determine output location.
                .by(message -> pathTemplate.extractValuesFrom(message.getAttributeMap()))
                .withDestinationCoder(ListCoder.of(StringUtf8Coder.of()))
                .via(Contextful.fn(format::encodeSingleMessage), TextIO.sink())
                .to(pathTemplate.staticPrefix)
                .withNaming(placeholderValues -> FileIO.Write.defaultNaming(
                    pathTemplate.replaceDynamicPart(placeholderValues), format.suffix()))));
  }

  protected static PTransform<PCollection<PubsubMessage>, PDone> writePubsub(
      ValueProvider<String> location) {
    return PTransform
        .compose(input -> input.apply("writePubsub", PubsubIO.writeMessages().to(location)));
  }

  protected static PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> writeBigQuery(
      ValueProvider<String> tableSpec) {
    return PTransform.compose((PCollection<PubsubMessage> input) -> {
      PubsubMessageToTableRow decodeTableRow = new PubsubMessageToTableRow();
      AsJsons<TableRow> encodeTableRow = AsJsons.of(TableRow.class);
      PCollectionTuple tableRows = input.apply("decodeTableRow", decodeTableRow);
      final WriteResult writeResult = tableRows.get(decodeTableRow.mainTag)
          .setCoder(TableRowJsonCoder.of()).apply("writeTableRows",
              BigQueryIO.writeTableRows().withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                  .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                  .skipInvalidRows()
                  .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                  .to(tableSpec));
      return PCollectionList.of(tableRows.get(decodeTableRow.errorTag))
          .and(writeResult.getFailedInserts().apply(encodeTableRow)
              .apply(InputFileFormat.text.decode()) // TODO: add error_{type,message} fields
              .get(DecodePubsubMessages.mainTag))
          .apply("flatten writeBigQuery error collections", Flatten.pCollections());
    });
  }

  /*
   * Public static fields and methods.
   */

  private static Create.Values<PubsubMessage> EMPTY_ERROR_COLLECTION = Create
      .empty(PubsubMessageWithAttributesCoder.of());
}
