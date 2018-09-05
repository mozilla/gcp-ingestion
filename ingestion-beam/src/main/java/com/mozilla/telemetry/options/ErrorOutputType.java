package com.mozilla.telemetry.options;

import com.mozilla.telemetry.Sink;
import com.mozilla.telemetry.transforms.CompositeTransform;
import com.mozilla.telemetry.transforms.Foreach;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public enum ErrorOutputType {
  stdout {
    /** Return a PTransform that prints errors to STDOUT; only for local running. */
    public PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options) {
      return CompositeTransform.of(input -> input
          .apply(FORMAT.encode())
          .apply(Foreach.string(System.out::println))
      );
    }
  },

  stderr {
    /** Return a PTransform that prints errors to STDERR; only for local running. */
    public PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options) {
      return CompositeTransform.of(input -> input
          .apply(FORMAT.encode())
          .apply(Foreach.string(System.err::println)));
    }
  },

  pubsub {
    /** Return a PTransform that writes to Google Pubsub. */
    public PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options) {
      return PubsubIO.writeMessages().to(options.getErrorOutput());
    }
  },

  file {
    /** Return a PTransform that writes errors to local or remote files. */
    public PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options) {
      return CompositeTransform.of(input -> input
          .apply(FORMAT.encode())
          .apply(Window.into(OutputType.parseWindow(options)))
          .apply(TextIO.write().to(options.getErrorOutput()).withWindowedWrites())
      );
    }
  };

  public static OutputFileFormat FORMAT = OutputFileFormat.json;

  public abstract PTransform<PCollection<PubsubMessage>, PDone> write(Sink.Options options);
}
