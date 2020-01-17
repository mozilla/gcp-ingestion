package com.mozilla.telemetry.options;

import com.mozilla.telemetry.io.Write;
import com.mozilla.telemetry.io.Write.BigQueryOutput;
import com.mozilla.telemetry.io.Write.FileOutput;
import com.mozilla.telemetry.io.Write.IgnoreOutput;
import com.mozilla.telemetry.io.Write.PrintOutput;
import com.mozilla.telemetry.transforms.Println;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow.TableRowFormat;
import com.mozilla.telemetry.transforms.WithErrors.Result;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
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

  ignore {

    /** Return a PTransform that prints messages to STDERR; only for local running. */
    public Write writeFailures(SinkOptions.Parsed options) {
      return new IgnoreOutput();
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
      final ValueProvider<String> inputValueProvider = options.getInput();
      final String inputType = options.getInputType().toString();
      final String jobName = options.getJobName();

      return new Write() {

        @Override
        public Result<PDone> expand(PCollection<PubsubMessage> input) {
          // We write errors from many pipelines to a single error topic, so the topic name itself
          // does not tell us provenance and we must encode that as attributes; see
          // https://github.com/mozilla/gcp-ingestion/issues/756
          return input.apply("AddPipelineAttributes",
              MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via(message -> {
                message = PubsubConstraints.ensureNonNull(message);
                Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
                Optional.ofNullable(inputValueProvider).filter(ValueProvider::isAccessible)
                    .map(ValueProvider::get).ifPresent(v -> attributes.put("input", v));
                attributes.put("input_type", inputType);
                attributes.put("job_name", jobName);
                return new PubsubMessage(message.getPayload(), attributes);
              }))
              .apply(new PubsubOutput(options.getErrorOutput(),
                  options.getErrorOutputPubsubCompression(),
                  PubsubConstraints.MAX_ENCODABLE_MESSAGE_BYTES));
        }
      };
    }
  },

  bigquery {

    /** Return a PTransform that writes to a BigQuery table. */
    public Write writeFailures(SinkOptions.Parsed options) {
      return new BigQueryOutput(options.getErrorOutput(), options.getErrorBqWriteMethod(),
          options.getParsedErrorBqTriggeringFrequency(), options.getInputType(),
          options.getErrorBqNumFileShards(), StaticValueProvider.of(null),
          StaticValueProvider.of(null), options.getSchemasLocation(),
          options.getSchemaAliasesLocation(), StaticValueProvider.of(TableRowFormat.raw),
          options.getErrorBqPartitioningField(), options.getErrorBqClusteringFields());
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
              .apply("WriteFailures", writeFailures(options));
        }
      };
    }
  }
}
