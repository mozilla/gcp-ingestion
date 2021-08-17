package com.mozilla.telemetry.io;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.beam.PublisherOptions;
import com.google.cloud.pubsublite.beam.PubsubLiteIO;
import com.mozilla.telemetry.avro.BinaryRecordFormatter;
import com.mozilla.telemetry.avro.GenericRecordBinaryEncoder;
import com.mozilla.telemetry.avro.PubsubMessageRecordFormatter;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.AvroSchemaStore;
import com.mozilla.telemetry.ingestion.core.util.DerivedAttributesMap;
import com.mozilla.telemetry.options.BigQueryWriteMethod;
import com.mozilla.telemetry.options.InputType;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.CompressPayload;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.KeyByBigQueryTableDestination;
import com.mozilla.telemetry.transforms.LimitPayloadSize;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.transforms.PubsubLiteCompat;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow.TableRowFormat;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.DynamicPathTemplate;
import com.mozilla.telemetry.util.NoColonFileNaming;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
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
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;

/**
 * Implementations of writing to the sinks enumerated in {@link
 * com.mozilla.telemetry.options.OutputType}.
 */
public abstract class Write
    extends PTransform<PCollection<PubsubMessage>, WithFailures.Result<PDone, PubsubMessage>> {

  /** Implementation of printing to STDOUT or STDERR. */
  public static class PrintOutput extends Write {

    private final OutputFileFormat format;
    private final PTransform<PCollection<String>, PDone> output;

    public PrintOutput(OutputFileFormat format, PTransform<PCollection<String>, PDone> output) {
      this.format = format;
      this.output = output;
    }

    @Override
    public WithFailures.Result<PDone, PubsubMessage> expand(PCollection<PubsubMessage> input) {
      PDone output = input //
          .apply("endcode PubsubMessages as strings", format.encode()) //
          .apply("print", this.output);
      return WithFailures.Result.of(output, EmptyErrors.in(input.getPipeline()));
    }

  }

  /** Implementation of ignoring messages and sending to no output. */
  public static class IgnoreOutput extends Write {

    public IgnoreOutput() {
    }

    @Override
    public WithFailures.Result<PDone, PubsubMessage> expand(PCollection<PubsubMessage> input) {
      return WithFailures.Result.of(PDone.in(input.getPipeline()),
          EmptyErrors.in(input.getPipeline()));
    }

  }

  /**
   * Implementation of writing to local or remote files.
   *
   * <p>For details of the intended behavior for file paths, see:
   * https://github.com/mozilla/gcp-ingestion/tree/main/ingestion-beam#output-path-specification
   */
  public static class FileOutput extends Write {

    private final String outputPrefix;
    private final OutputFileFormat format;
    private final Duration windowDuration;
    private final Integer numShards;
    private final Compression compression;
    private final InputType inputType;

    /** Public constructor. */
    public FileOutput(String outputPrefix, OutputFileFormat format, Duration windowDuration,
        Integer numShards, Compression compression, InputType inputType) {
      this.outputPrefix = outputPrefix;
      this.format = format;
      this.windowDuration = windowDuration;
      this.numShards = numShards;
      this.compression = compression;
      this.inputType = inputType;
    }

    @Override
    public WithFailures.Result<PDone, PubsubMessage> expand(PCollection<PubsubMessage> input) {
      DynamicPathTemplate pathTemplate = new DynamicPathTemplate(outputPrefix);
      String staticPrefix = pathTemplate.staticPrefix;

      FileIO.Write<List<String>, PubsubMessage> write = FileIO
          .<List<String>, PubsubMessage>writeDynamic()
          // We can't pass the attribute map to by() directly since MapCoder isn't
          // deterministic;
          // instead, we extract an ordered list of the needed placeholder values.
          // That list is later available to withNaming() to determine output location.
          .by(message -> pathTemplate
              .extractValuesFrom(DerivedAttributesMap.of(message.getAttributeMap())))
          .withDestinationCoder(ListCoder.of(StringUtf8Coder.of())) //
          .withCompression(compression) //
          .via(Contextful.fn(format::encodeSingleMessage), TextIO.sink()) //
          .to(staticPrefix) //
          .withNaming(placeholderValues -> NoColonFileNaming
              .defaultNaming(pathTemplate.replaceDynamicPart(placeholderValues), format.suffix()));

      if (inputType == InputType.pubsub) {
        // withNumShards disables runner-determined sharding, so we need to be careful to pass this
        // only for streaming input (where runner-determined sharding is not an option).
        write = write.withNumShards(numShards);
      }

      input //
          .apply(Window.<PubsubMessage>into(FixedWindows.of(windowDuration))
              // We allow lateness up to the maximum Cloud Pub/Sub retention of 7 days documented in
              // https://cloud.google.com/pubsub/docs/subscriber
              .withAllowedLateness(Duration.standardDays(7)) //
              .discardingFiredPanes())
          .apply(write);
      return WithFailures.Result.of(PDone.in(input.getPipeline()),
          EmptyErrors.in(input.getPipeline()));
    }
  }

  /**
   * Implementation of writing to local or remote files.
   *
   * <p>For details of the intended behavior for file paths, see:
   * https://github.com/mozilla/gcp-ingestion/tree/main/ingestion-beam#output-path-specification
   */
  public static class AvroOutput extends Write {

    private final String outputPrefix;
    private final Duration windowDuration;
    private final Integer numShards;
    private final Compression compression;
    private final InputType inputType;
    private final String schemasLocation;
    private final DynamicPathTemplate pathTemplate;
    private final PubsubMessageRecordFormatter formatter = new PubsubMessageRecordFormatter();
    private final GenericRecordBinaryEncoder binaryEncoder = new GenericRecordBinaryEncoder();
    private final BinaryRecordFormatter binaryFormatter = new BinaryRecordFormatter();
    private final TupleTag<PubsubMessage> successTag = new TupleTag<PubsubMessage>() {
    };
    private final TupleTag<PubsubMessage> errorTag = new TupleTag<PubsubMessage>() {
    };

    /** Public constructor. */
    public AvroOutput(String outputPrefix, Duration windowDuration, Integer numShards,
        Compression compression, InputType inputType, String schemasLocation) {
      this.outputPrefix = outputPrefix;
      this.windowDuration = windowDuration;
      this.numShards = numShards;
      this.compression = compression;
      this.inputType = inputType;
      this.schemasLocation = schemasLocation;
      this.pathTemplate = new DynamicPathTemplate(outputPrefix);
    }

    class AvroEncoder extends DoFn<PubsubMessage, PubsubMessage> {

      private transient AvroSchemaStore store;

      private AvroSchemaStore getStore() {
        if (store == null) {
          store = AvroSchemaStore.of(schemasLocation, BeamFileInputStream::open);
        }
        return store;
      }

      @ProcessElement
      public void processElement(ProcessContext ctx) {
        PubsubMessage message = ctx.element();
        Map<String, String> attributes = message.getAttributeMap();
        try {
          Schema schema = getStore().getSchema(attributes);
          GenericRecord record = formatter.formatRecord(message, schema);
          byte[] avroPayload = binaryEncoder.encodeRecord(record, schema);
          ctx.output(successTag, new PubsubMessage(avroPayload, attributes));
        } catch (Exception e) {
          ctx.output(errorTag, FailureMessage.of(this, message, e));
        }
      }

      private AvroIO.Sink<PubsubMessage> getSink(List<String> dest) {
        Map<String, String> attributes = pathTemplate.getPlaceholderAttributes(dest);
        Schema schema = getStore().getSchema(attributes);
        return AvroIO.sinkViaGenericRecords(schema, binaryFormatter);
      }
    }

    @Override
    public WithFailures.Result<PDone, PubsubMessage> expand(PCollection<PubsubMessage> input) {
      String staticPrefix = pathTemplate.staticPrefix;

      List<String> placeholders = pathTemplate.getPlaceholderNames();
      if (!placeholders
          .containsAll(Arrays.asList("document_namespace", "document_type", "document_version"))) {
        throw new RuntimeException(
            "Path template must contain document namespace, type, and version");
      }

      AvroEncoder encoder = new AvroEncoder();

      // A ParDo is opted over a PTransform extending MapElementsWithErrors.
      // While this leads to manual error handling with output-tags, this allows
      // for side-input of the singleton SchemaStore PCollection.
      ParDo.MultiOutput<PubsubMessage, PubsubMessage> encodePayloadAsAvro = ParDo.of(encoder)
          .withOutputTags(successTag, TupleTagList.of(errorTag));

      FileIO.Write<List<String>, PubsubMessage> write = FileIO
          .<List<String>, PubsubMessage>writeDynamic() //
          .by(message -> pathTemplate.extractValuesFrom(message.getAttributeMap()))
          .withDestinationCoder(ListCoder.of(StringUtf8Coder.of())) //
          .withCompression(compression) //
          .via(Contextful.fn(encoder::getSink)) //
          .to(staticPrefix) //
          .withNaming(placeholderValues -> NoColonFileNaming
              .defaultNaming(pathTemplate.replaceDynamicPart(placeholderValues), ".avro"));

      if (inputType == InputType.pubsub) {
        // withNumShards disables runner-determined sharding, so we need to be careful to pass this
        // only for streaming input (where runner-determined sharding is not an option).
        write = write.withNumShards(numShards);
      }

      // Without this, we may run into `Inputs to Flatten had incompatible window windowFns`
      Window<PubsubMessage> window = Window.<PubsubMessage>into(FixedWindows.of(windowDuration))
          // We allow lateness up to the maximum Cloud Pub/Sub retention of 7 days documented in
          // https://cloud.google.com/pubsub/docs/subscriber
          .withAllowedLateness(Duration.standardDays(7)) //
          .discardingFiredPanes();

      PCollectionTuple results = input.apply("encodePayloadAsAvro", encodePayloadAsAvro);
      results.get(successTag).apply(window).apply(write);

      return WithFailures.Result.of(PDone.in(input.getPipeline()), results.get(errorTag));
    }
  }

  /** Implementation of writing to a Pub/Sub topic. */
  public static class PubsubOutput extends Write {

    private final String topic;
    private final Compression compression;
    private final int maxCompressedBytes;

    /** Constructor. */
    public PubsubOutput(String topic, Compression compression, int maxCompressedBytes) {
      this.topic = topic;
      this.compression = compression;
      this.maxCompressedBytes = maxCompressedBytes;
    }

    /** Constructor. */
    public PubsubOutput(String topic, Compression compression) {
      this(topic, compression, Integer.MAX_VALUE);
    }

    @Override
    public WithFailures.Result<PDone, PubsubMessage> expand(PCollection<PubsubMessage> input) {
      PDone done = input //
          .apply(CompressPayload.of(compression).withMaxCompressedBytes(maxCompressedBytes)) //
          .apply(PubsubConstraints.truncateAttributes()) //
          .apply(PubsubIO.writeMessages().to(topic));
      return WithFailures.Result.of(done, EmptyErrors.in(input.getPipeline()));
    }
  }

  /** Implementation of writing to a Pub/Sub topic. */
  public static class PubsubLiteOutput extends Write {

    private final TopicPath path;
    private final Compression compression;
    private final int maxCompressedBytes;

    /** Constructor. */
    public PubsubLiteOutput(String topic, Compression compression, int maxCompressedBytes) {
      this.path = TopicPath.parse(topic);
      this.compression = compression;
      this.maxCompressedBytes = maxCompressedBytes;
    }

    /** Constructor. */
    public PubsubLiteOutput(String topic, Compression compression) {
      this(topic, compression, Integer.MAX_VALUE);
    }

    @Override
    public WithFailures.Result<PDone, PubsubMessage> expand(PCollection<PubsubMessage> input) {
      PDone done = input //
          .apply(CompressPayload.of(compression).withMaxCompressedBytes(maxCompressedBytes)) //
          .apply(PubsubConstraints.truncateAttributes()) //
          .apply(PubsubLiteCompat.toPubsubLite()) //
          .apply(PubsubLiteIO.write(PublisherOptions.newBuilder().setTopicPath(path).build()));
      return WithFailures.Result.of(done, EmptyErrors.in(input.getPipeline()));
    }
  }

  /** Implementation of writing to BigQuery tables. */
  public static class BigQueryOutput extends Write {

    private final String tableSpecTemplate;
    private final BigQueryWriteMethod writeMethod;
    private final Duration triggeringFrequency;
    private final InputType inputType;
    private final int numShards;
    private final long maxBytesPerPartition;
    private final List<String> streamingDocTypes;
    private final List<String> strictSchemaDocTypes;
    private final String schemasLocation;
    private final TableRowFormat tableRowFormat;
    private final String partitioningField;
    private final List<String> clusteringFields;

    /** Public constructor. */
    public BigQueryOutput(String tableSpecTemplate, BigQueryWriteMethod writeMethod,
        Duration triggeringFrequency, InputType inputType, int numShards, long maxBytesPerPartition,
        List<String> streamingDocTypes, List<String> strictSchemaDocTypes, String schemasLocation,
        TableRowFormat tableRowFormat, String partitioningField, List<String> clusteringFields) {
      this.tableSpecTemplate = tableSpecTemplate;
      this.writeMethod = writeMethod;
      this.triggeringFrequency = triggeringFrequency;
      this.inputType = inputType;
      this.numShards = numShards;
      this.maxBytesPerPartition = maxBytesPerPartition;
      this.streamingDocTypes = Optional.ofNullable(streamingDocTypes)
          .orElse(Collections.emptyList());
      this.strictSchemaDocTypes = Optional.ofNullable(strictSchemaDocTypes)
          .orElse(Collections.emptyList());
      this.schemasLocation = schemasLocation;
      this.tableRowFormat = tableRowFormat;
      this.partitioningField = Optional.ofNullable(partitioningField)
          .orElse(Attribute.SUBMISSION_TIMESTAMP);
      this.clusteringFields = Optional.ofNullable(clusteringFields)
          .orElse(Collections.singletonList(Attribute.SUBMISSION_TIMESTAMP));
    }

    @Override
    public WithFailures.Result<PDone, PubsubMessage> expand(PCollection<PubsubMessage> input) {
      final List<PCollection<PubsubMessage>> failureCollections = new ArrayList<>();
      KeyByBigQueryTableDestination keyByBigQueryTableDestination = KeyByBigQueryTableDestination
          .of(tableSpecTemplate, partitioningField, clusteringFields);

      input = input //
          .apply("LimitPayloadSize", LimitPayloadSize.toBytes(writeMethod.maxPayloadBytes))
          .failuresTo(failureCollections);

      // When writing to live tables, we expect the input is uncompressed and we partition to
      // streaming vs. file loads based on uncompressed size, but we then want to compress again
      // before sending to BigQueryIO to save on I/O costs during several GBK operations;
      // the payload will again be decompressed in the formatFunction passed to BigQueryIO.
      final CompressPayload maybeCompress = CompressPayload.of(
          tableRowFormat == TableRowFormat.payload ? Compression.GZIP : Compression.UNCOMPRESSED);

      final PubsubMessageToTableRow pubsubMessageToTableRow = PubsubMessageToTableRow
          .of(strictSchemaDocTypes, schemasLocation, tableRowFormat);
      final BigQueryIO.Write<KV<TableDestination, PubsubMessage>> baseWriteTransform = BigQueryIO //
          .<KV<TableDestination, PubsubMessage>>write() //
          .withFormatFunction(pubsubMessageToTableRow::kvToTableRow) //
          .to((ValueInSingleWindow<KV<TableDestination, PubsubMessage>> vsw) -> vsw.getValue()
              .getKey())
          .withClustering() //
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER) //
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND) //
          .ignoreUnknownValues();

      final Optional<PCollection<PubsubMessage>> streamingInput;
      final Optional<PCollection<PubsubMessage>> fileLoadsInput;
      if (writeMethod == BigQueryWriteMethod.streaming) {
        streamingInput = Optional.of(input);
        fileLoadsInput = Optional.empty();
      } else if (writeMethod == BigQueryWriteMethod.file_loads) {
        streamingInput = Optional.empty();
        fileLoadsInput = Optional.of(input);
      } else {
        // writeMethod is mixed.
        final PCollectionList<PubsubMessage> partitioned = input //
            .apply("PartitionStreamingVsFileLoads", Partition.of(2, //
                (message, numPartitions) -> {
                  message = PubsubConstraints.ensureNonNull(message);
                  final boolean shouldStream;
                  if (streamingDocTypes.contains("*")) {
                    shouldStream = true;
                  } else {
                    final String namespace = message.getAttribute("document_namespace");
                    final String docType = message.getAttribute("document_type");
                    if (namespace == null || docType == null) {
                      shouldStream = false;
                    } else {
                      shouldStream = streamingDocTypes.contains(namespace + "/" + docType);
                    }
                  }
                  if (shouldStream && message
                      .getPayload().length < BigQueryWriteMethod.streaming.maxPayloadBytes) {
                    return 0;
                  } else {
                    return 1;
                  }
                }));
        streamingInput = Optional.of(partitioned.get(0));
        fileLoadsInput = Optional.of(partitioned.get(1));
      }

      streamingInput.ifPresent(messages -> {
        WriteResult writeResult = messages //
            .apply(maybeCompress) //
            .apply(keyByBigQueryTableDestination) //
            .failuresTo(failureCollections) //
            .apply(baseWriteTransform //
                .withMethod(BigQueryWriteMethod.streaming.method)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()) //
                .skipInvalidRows() //
                .withExtendedErrorInfo());
        failureCollections
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
                  TableRow row = bqie.getRow();
                  row.setFactory(JacksonFactory.getDefaultInstance());
                  byte[] payload = row.toString().getBytes(StandardCharsets.UTF_8);
                  return new PubsubMessage(payload, attributes);
                })));
      });

      fileLoadsInput.ifPresent(messages -> {
        BigQueryIO.Write<KV<TableDestination, PubsubMessage>> fileLoadsWrite = baseWriteTransform
            .withMethod(BigQueryWriteMethod.file_loads.method)
            .withMaxBytesPerPartition(maxBytesPerPartition);
        if (inputType == InputType.pubsub) {
          // When using the file_loads method of inserting to BigQuery, BigQueryIO requires
          // triggering frequency if the input PCollection is unbounded (which is the case for
          // pubsub), but forbids the option if the input PCollection is bounded.
          fileLoadsWrite = fileLoadsWrite.withTriggeringFrequency(triggeringFrequency) //
              .withNumFileShards(numShards);
        }
        messages //
            .apply(maybeCompress) //
            .apply(keyByBigQueryTableDestination) //
            .failuresTo(failureCollections) //
            .apply(fileLoadsWrite);
      });

      PCollection<PubsubMessage> failureCollection = PCollectionList.of(failureCollections)
          .apply("Flatten bigquery errors", Flatten.pCollections());

      return WithFailures.Result.of(PDone.in(input.getPipeline()), failureCollection);
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
