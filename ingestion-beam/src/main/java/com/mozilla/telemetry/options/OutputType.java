package com.mozilla.telemetry.options;

import com.mozilla.telemetry.io.Write;
import com.mozilla.telemetry.io.Write.AvroOutput;
import com.mozilla.telemetry.io.Write.BigQueryOutput;
import com.mozilla.telemetry.io.Write.FileOutput;
import com.mozilla.telemetry.io.Write.IgnoreOutput;
import com.mozilla.telemetry.io.Write.PrintOutput;
import com.mozilla.telemetry.io.Write.PubsubLiteOutput;
import com.mozilla.telemetry.io.Write.PubsubOutput;
import com.mozilla.telemetry.transforms.Println;

/**
 * Enumeration of output types that each provide a {@code write} method.
 */
public enum OutputType {
  stdout {

    /** Return a PTransform that prints messages to STDOUT; only for local running. */
    public Write write(SinkOptions.Parsed options) {
      return new PrintOutput(options.getOutputFileFormat(), Println.stdout());
    }
  },

  stderr {

    /** Return a PTransform that prints messages to STDERR; only for local running. */
    public Write write(SinkOptions.Parsed options) {
      return new PrintOutput(options.getOutputFileFormat(), Println.stderr());
    }
  },

  ignore {

    /** Return a PTransform that prints messages to STDERR; only for local running. */
    public Write write(SinkOptions.Parsed options) {
      return new IgnoreOutput();
    }
  },

  file {

    /** Return a PTransform that writes to local or remote files. */
    public Write write(SinkOptions.Parsed options) {
      return new FileOutput(options.getOutput(), options.getOutputFileFormat(),
          options.getParsedWindowDuration(), options.getOutputNumShards(),
          options.getOutputFileCompression(), options.getInputType());
    }
  },

  avro {

    /** Return a PTransform that writes to local or remote files. */
    public Write write(SinkOptions.Parsed options) {
      return new AvroOutput(options.getOutput(), options.getParsedWindowDuration(),
          options.getOutputNumShards(), options.getOutputFileCompression(), options.getInputType(),
          options.getSchemasLocation());
    }
  },

  pubsub {

    /** Return a PTransform that writes to Google Pubsub. */
    public Write write(SinkOptions.Parsed options) {
      return new PubsubOutput(options.getOutput(), options.getOutputPubsubCompression());
    }
  },

  pubsub_lite {

    /** Return a PTransform that writes to Google Pubsub. */
    public Write write(SinkOptions.Parsed options) {
      return new PubsubLiteOutput(options.getOutput(), options.getOutputPubsubCompression());
    }
  },

  bigquery {

    /** Return a PTransform that writes to a BigQuery table and collects failed inserts. */
    public Write write(SinkOptions.Parsed options) {
      return new BigQueryOutput(options.getOutput(), options.getBqWriteMethod(),
          options.getParsedBqTriggeringFrequency(), options.getInputType(),
          options.getBqNumFileShards(), options.getBqMaxBytesPerPartition(),
          options.getBqStreamingDocTypes(), options.getBqStrictSchemaDocTypes(),
          options.getSchemasLocation(), options.getOutputTableRowFormat(),
          options.getBqPartitioningField(), options.getBqClusteringFields());
    }
  };

  /**
   * Each case in the enum must implement this method to define how to write out messages.
   *
   * @return A PCollection of failure messages about data that could not be written
   */
  public abstract Write write(SinkOptions.Parsed options);

}
