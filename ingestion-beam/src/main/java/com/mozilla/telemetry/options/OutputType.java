/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.google.api.services.bigquery.model.TableRow;
import com.mozilla.telemetry.transforms.Println;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow;
import com.mozilla.telemetry.transforms.ResultWithErrors;
import com.mozilla.telemetry.util.DynamicPathTemplate;
import java.util.List;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
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
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.joda.time.Duration;

/**
 * Enumeration of output types that each provide a {@code write} method.
 */
public enum OutputType {
  stdout {

    /**
     * Return a PTransform that prints messages to STDOUT; only for local running.
     */
    public PTransform<PCollection<PubsubMessage>, ResultWithErrors<? extends POutput>> write(
        SinkOptions.Parsed options) {
      return new EmptyErrors(print(options.getOutputFileFormat(), Println.stdout()));
    }
  },

  stderr {

    /** Return a PTransform that prints messages to STDERR; only for local running. */
    public PTransform<PCollection<PubsubMessage>, ResultWithErrors<? extends POutput>> write(
        SinkOptions.Parsed options) {
      return new EmptyErrors(print(options.getOutputFileFormat(), Println.stderr()));
    }
  },

  file {

    /** Return a PTransform that writes to local or remote files. */
    public PTransform<PCollection<PubsubMessage>, ResultWithErrors<? extends POutput>> write(
        SinkOptions.Parsed options) {
      return new EmptyErrors(writeFile(options.getOutput(), options.getOutputFileFormat(),
          options.getParsedWindowDuration(), options.getOutputNumShards()));
    }
  },

  pubsub {

    /** Return a PTransform that writes to Google Pubsub. */
    public PTransform<PCollection<PubsubMessage>, ResultWithErrors<? extends POutput>> write(
        SinkOptions.Parsed options) {
      return new EmptyErrors(writePubsub(options.getOutput()));
    }
  },

  bigquery {

    /** Return a PTransform that writes to a BigQuery table and collects failed inserts. */
    public PTransform<PCollection<PubsubMessage>, ResultWithErrors<? extends POutput>> write(
        SinkOptions.Parsed options) {
      return PTransform.compose(input -> input.apply(writeBigQuery(options.getOutput())));
    }
  };

  /**
   * Each case in the enum must implement this method to define how to write out messages.
   *
   * @return A PCollection of failure messages about data that could not be written.
   */
  public abstract PTransform<PCollection<PubsubMessage>, ResultWithErrors<? extends POutput>> write(
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
  protected static PTransform<PCollection<PubsubMessage>, WriteFilesResult<List<String>>> writeFile(
      ValueProvider<String> outputPrefix, OutputFileFormat format, Duration windowDuration,
      int numShards) {
    DynamicPathTemplate pathTemplate = new DynamicPathTemplate(outputPrefix.get());
    return PTransform
        .compose(input -> input.apply("fixedWindows", Window.into(FixedWindows.of(windowDuration)))
            .apply("writeFile", FileIO.<List<String>, PubsubMessage>writeDynamic()
                // We can't pass the attribute map to by() directly since MapCoder isn't
                // deterministic;
                // instead, we extract an ordered list of the needed placeholder values.
                // That list is later available to withNaming() to determine output location.
                .by(message -> pathTemplate.extractValuesFrom(message.getAttributeMap()))
                .withDestinationCoder(ListCoder.of(StringUtf8Coder.of())).withNumShards(numShards)
                .via(Contextful.fn(format::encodeSingleMessage), TextIO.sink())
                .to(pathTemplate.staticPrefix).withNaming(placeholderValues -> Write.defaultNaming(
                    pathTemplate.replaceDynamicPart(placeholderValues), format.suffix()))));
  }

  protected static PTransform<PCollection<PubsubMessage>, PDone> writePubsub(
      ValueProvider<String> location) {
    return PubsubIO.writeMessages().to(location);
  }

  protected static PTransform<PCollection<PubsubMessage>, ResultWithErrors<WriteResult>> writeBigQuery(
      ValueProvider<String> tableSpec) {
    return PTransform.compose((PCollection<PubsubMessage> input) -> {
      PubsubMessageToTableRow decodeTableRow = new PubsubMessageToTableRow();
      AsJsons<TableRow> encodeTableRow = AsJsons.of(TableRow.class);
      ResultWithErrors<PCollection<TableRow>> tableRows = input.apply("decodeTableRow",
          decodeTableRow);
      final WriteResult writeResult = tableRows.output().setCoder(TableRowJsonCoder.of()).apply(
          "writeTableRows",
          BigQueryIO.writeTableRows().withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
              .skipInvalidRows()
              .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()).to(tableSpec));
      PCollection<PubsubMessage> errorCollection = PCollectionList.of(tableRows.errors())
          .and(writeResult.getFailedInserts().apply(encodeTableRow)
              .apply(InputFileFormat.text.decode()) // TODO: add error_{type,message} fields
              .output())
          .apply("flatten writeBigQuery error collections", Flatten.pCollections());
      return ResultWithErrors.of(writeResult, errorCollection);
    });
  }

  /*
   * Private helpers.
   */

  private static class EmptyErrors
      extends PTransform<PCollection<PubsubMessage>, ResultWithErrors<? extends POutput>> {

    private final PTransform<PCollection<PubsubMessage>, ? extends POutput> inner;

    public EmptyErrors(PTransform<PCollection<PubsubMessage>, ? extends POutput> inner) {
      this.inner = inner;
    }

    @Override
    public ResultWithErrors<? extends POutput> expand(PCollection<PubsubMessage> input) {
      return ResultWithErrors.of(input.apply(inner),
          input.getPipeline().apply(Create.empty(PubsubMessageWithAttributesCoder.of())));
    }
  }

}
