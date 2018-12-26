/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableRow;
import com.mozilla.telemetry.transforms.Println;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow;
import com.mozilla.telemetry.transforms.ResultWithErrors;
import com.mozilla.telemetry.util.DerivedAttributesMap;
import com.mozilla.telemetry.util.DynamicPathTemplate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
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
    ValueProvider<DynamicPathTemplate> pathTemplate = NestedValueProvider.of(outputPrefix,
        DynamicPathTemplate::new);
    ValueProvider<String> staticPrefix = NestedValueProvider.of(pathTemplate,
        value -> value.staticPrefix);
    return PTransform
        .compose(
            input -> input.apply("fixedWindows", Window.into(FixedWindows.of(windowDuration)))
                .apply("writeFile", FileIO.<List<String>, PubsubMessage>writeDynamic()
                    // We can't pass the attribute map to by() directly since MapCoder isn't
                    // deterministic;
                    // instead, we extract an ordered list of the needed placeholder values.
                    // That list is later available to withNaming() to determine output location.
                    .by(message -> pathTemplate.get()
                        .extractValuesFrom(DerivedAttributesMap.of(message.getAttributeMap())))
                    .withDestinationCoder(ListCoder.of(StringUtf8Coder.of()))
                    .withNumShards(numShards)
                    .via(Contextful.fn(format::encodeSingleMessage), TextIO.sink()).to(staticPrefix)
                    .withNaming(placeholderValues -> Write.defaultNaming(
                        pathTemplate.get().replaceDynamicPart(placeholderValues),
                        format.suffix()))));
  }

  protected static PTransform<PCollection<PubsubMessage>, PDone> writePubsub(
      ValueProvider<String> location) {
    return PubsubIO.writeMessages().to(location);
  }

  protected static PTransform<PCollection<PubsubMessage>, ResultWithErrors<WriteResult>> writeBigQuery(
      ValueProvider<String> tableSpecTemplate) {
    return PTransform.compose((PCollection<PubsubMessage> input) -> {
      ResultWithErrors<PCollection<KV<TableDestination, TableRow>>> tableRows = input
          .apply(PubsubMessageToTableRow.of(tableSpecTemplate));

      WriteResult writeResult = tableRows.output().apply(BigQueryIO //
          .<KV<TableDestination, TableRow>>write() //
          .withFormatFunction(KV::getValue) //
          .to((ValueInSingleWindow<KV<TableDestination, TableRow>> vsw) -> vsw.getValue().getKey())
          .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS) //
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER) //
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND).skipInvalidRows()
          .withExtendedErrorInfo().ignoreUnknownValues()
          .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

      PCollection<PubsubMessage> failedInserts = writeResult.getFailedInsertsWithErr()
          .apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class))
              .via((BigQueryInsertError bqie) -> {
                Map<String, String> attributes = new HashMap<>();
                attributes.put("error_type", "failed_insert");
                attributes.put("error_table",
                    String.format("%s:%s.%s", bqie.getTable().getProjectId(),
                        bqie.getTable().getDatasetId(), bqie.getTable().getTableId()));
                if (!bqie.getError().getErrors().isEmpty()) {
                  // We pull out the first error to top-level attributes.
                  ErrorProto errorProto = bqie.getError().getErrors().get(0);
                  attributes.put("error_message", errorProto.getMessage());
                  attributes.put("error_location", errorProto.getLocation());
                  attributes.put("error_reason", errorProto.getReason());
                }
                if (bqie.getError().getErrors().size() > 1) {
                  // If there are additional errors, we include the entire JSON response.
                  attributes.put("insert_errors", bqie.getError().toString());
                }
                byte[] payload = bqie.getRow().toString().getBytes();
                return new PubsubMessage(payload, attributes);
              }));
      PCollection<PubsubMessage> errorCollection = PCollectionList.of(tableRows.errors())
          .and(failedInserts).apply(Flatten.pCollections());

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
