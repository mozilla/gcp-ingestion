/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.io;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableRow;
import com.mozilla.telemetry.options.BigQueryWriteMethod;
import com.mozilla.telemetry.options.InputType;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.CompressPayload;
import com.mozilla.telemetry.transforms.LimitPayloadSize;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow;
import com.mozilla.telemetry.transforms.WithErrors;
import com.mozilla.telemetry.util.DerivedAttributesMap;
import com.mozilla.telemetry.util.DynamicPathTemplate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
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
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;

/**
 * Implementations of writing to the sinks enumerated in {@link
 * com.mozilla.telemetry.options.OutputType}.
 */
public abstract class Write
    extends PTransform<PCollection<PubsubMessage>, WithErrors.Result<PDone>> {

  /** Implementation of printing to STDOUT or STDERR. */
  public static class PrintOutput extends Write {

    private final OutputFileFormat format;
    private final PTransform<PCollection<String>, PDone> output;

    public PrintOutput(OutputFileFormat format, PTransform<PCollection<String>, PDone> output) {
      this.format = format;
      this.output = output;
    }

    @Override
    public WithErrors.Result<PDone> expand(PCollection<PubsubMessage> input) {
      PDone output = input //
          .apply("endcode PubsubMessages as strings", format.encode()) //
          .apply("print", this.output);
      return WithErrors.Result.of(output, EmptyErrors.in(input.getPipeline()));
    }

  }

  /**
   * Implementation of writing to local or remote files.
   *
   * <p>For details of the intended behavior for file paths, see:
   * https://github.com/mozilla/gcp-ingestion/tree/master/ingestion-beam#output-path-specification
   */
  public static class FileOutput extends Write {

    private final ValueProvider<String> outputPrefix;
    private final OutputFileFormat format;
    private final Duration windowDuration;
    private final ValueProvider<Integer> numShards;
    private final Compression compression;
    private final InputType inputType;

    /** Public constructor. */
    public FileOutput(ValueProvider<String> outputPrefix, OutputFileFormat format,
        Duration windowDuration, ValueProvider<Integer> numShards, Compression compression,
        InputType inputType) {
      this.outputPrefix = outputPrefix;
      this.format = format;
      this.windowDuration = windowDuration;
      this.numShards = numShards;
      this.compression = compression;
      this.inputType = inputType;
    }

    @Override
    public WithErrors.Result<PDone> expand(PCollection<PubsubMessage> input) {
      ValueProvider<DynamicPathTemplate> pathTemplate = NestedValueProvider.of(outputPrefix,
          DynamicPathTemplate::new);
      ValueProvider<String> staticPrefix = NestedValueProvider.of(pathTemplate,
          value -> value.staticPrefix);

      FileIO.Write<List<String>, PubsubMessage> write = FileIO
          .<List<String>, PubsubMessage>writeDynamic()
          // We can't pass the attribute map to by() directly since MapCoder isn't
          // deterministic;
          // instead, we extract an ordered list of the needed placeholder values.
          // That list is later available to withNaming() to determine output location.
          .by(message -> pathTemplate.get()
              .extractValuesFrom(DerivedAttributesMap.of(message.getAttributeMap())))
          .withDestinationCoder(ListCoder.of(StringUtf8Coder.of())) //
          .withCompression(compression) //
          .via(Contextful.fn(format::encodeSingleMessage), TextIO.sink()) //
          .to(staticPrefix) //
          .withNaming(placeholderValues -> FileIO.Write.defaultNaming(
              pathTemplate.get().replaceDynamicPart(placeholderValues), format.suffix()));

      if (inputType == InputType.pubsub) {
        // Passing a ValueProvider to withNumShards disables runner-determined sharding, so we
        // need to be careful to pass this only for streaming input (where runner-determined
        // sharding is not an option).
        write = write.withNumShards(numShards);
      }

      input //
          .apply(Window.<PubsubMessage>into(FixedWindows.of(windowDuration))
              // We allow lateness up to the maximum Cloud Pub/Sub retention of 7 days documented in
              // https://cloud.google.com/pubsub/docs/subscriber
              .withAllowedLateness(Duration.standardDays(7)) //
              .discardingFiredPanes())
          .apply(write);
      return WithErrors.Result.of(PDone.in(input.getPipeline()),
          EmptyErrors.in(input.getPipeline()));
    }
  }

  /** Implementation of writing to a Pub/Sub topic. */
  public static class PubsubOutput extends Write {

    private final ValueProvider<String> topic;
    private final ValueProvider<Compression> compression;
    private final int maxCompressedBytes;

    /** Constructor. */
    public PubsubOutput(ValueProvider<String> topic, ValueProvider<Compression> compression,
        int maxCompressedBytes) {
      this.topic = topic;
      this.compression = compression;
      this.maxCompressedBytes = maxCompressedBytes;
    }

    /** Constructor. */
    public PubsubOutput(ValueProvider<String> topic, ValueProvider<Compression> compression) {
      this(topic, compression, Integer.MAX_VALUE);
    }

    @Override
    public WithErrors.Result<PDone> expand(PCollection<PubsubMessage> input) {
      PDone done = input //
          .apply(CompressPayload.of(compression).withMaxCompressedBytes(maxCompressedBytes)) //
          .apply(PubsubConstraints.truncateAttributes()) //
          .apply(PubsubIO.writeMessages().to(topic));
      return WithErrors.Result.of(done, EmptyErrors.in(input.getPipeline()));
    }
  }

  /** Implementation of writing to BigQuery tables. */
  public static class BigQueryOutput extends Write {

    private final ValueProvider<String> tableSpecTemplate;
    private final BigQueryWriteMethod writeMethod;
    private final Duration triggeringFrequency;
    private final InputType inputType;
    private final int numShards;

    /** Public constructor. */
    public BigQueryOutput(ValueProvider<String> tableSpecTemplate, BigQueryWriteMethod writeMethod,
        Duration triggeringFrequency, InputType inputType, int numShards) {
      this.tableSpecTemplate = tableSpecTemplate;
      this.writeMethod = writeMethod;
      this.triggeringFrequency = triggeringFrequency;
      this.inputType = inputType;
      this.numShards = numShards;
    }

    @Override
    public WithErrors.Result<PDone> expand(PCollection<PubsubMessage> input) {
      BigQueryIO.Write<KV<TableDestination, TableRow>> writeTransform = BigQueryIO //
          .<KV<TableDestination, TableRow>>write() //
          .withFormatFunction(KV::getValue) //
          .to((ValueInSingleWindow<KV<TableDestination, TableRow>> vsw) -> vsw.getValue().getKey())
          .withMethod(writeMethod.method)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER) //
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND) //
          .ignoreUnknownValues();

      if (writeMethod == BigQueryWriteMethod.streaming) {
        writeTransform = writeTransform
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()) //
            .skipInvalidRows() //
            .withExtendedErrorInfo();
      } else {
        if (inputType == InputType.pubsub) {
          // When using the file_loads method of inserting to BigQuery, BigQueryIO requires
          // triggering frequency if the input PCollection is unbounded (which is the case for
          // pubsub), but forbids the option if the input PCollection is bounded.
          writeTransform = writeTransform //
              .withTriggeringFrequency(triggeringFrequency) //
              .withNumFileShards(numShards);
        }
      }

      final List<PCollection<PubsubMessage>> errorCollections = new ArrayList<>();

      WriteResult writeResult = input //
          .apply(LimitPayloadSize.toMB(writeMethod.maxPayloadBytes)).errorsTo(errorCollections)
          .apply(PubsubMessageToTableRow.of(tableSpecTemplate)).errorsTo(errorCollections)
          .apply(writeTransform);

      if (writeMethod == BigQueryWriteMethod.streaming) {
        errorCollections
            .add(writeResult.getFailedInsertsWithErr().apply("Process failed inserts", MapElements
                .into(TypeDescriptor.of(PubsubMessage.class)).via((BigQueryInsertError bqie) -> {
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
                })));
      }

      PCollection<PubsubMessage> errorCollection = PCollectionList.of(errorCollections)
          .apply("Flatten bigquery errors", Flatten.pCollections());

      return WithErrors.Result.of(PDone.in(writeResult.getPipeline()), errorCollection);
    }
  }

  ////////

  /**
   * Transform producing an empty error collection for satisfying {@link Write}'s interface in cases
   * where the write implementation doesn't check for exceptions and produce an error collection
   * itself.
   */
  private static class EmptyErrors extends PTransform<PBegin, PCollection<PubsubMessage>> {

    /** Creates an empty error collection in the given pipeline. */
    public static PCollection<PubsubMessage> in(Pipeline pipeline) {
      return pipeline.apply(new EmptyErrors());
    }

    @Override
    public PCollection<PubsubMessage> expand(PBegin input) {
      return input.apply(Create.empty(PubsubMessageWithAttributesCoder.of()));
    }
  }

}
