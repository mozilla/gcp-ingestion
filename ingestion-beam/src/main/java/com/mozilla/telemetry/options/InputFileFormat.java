package com.mozilla.telemetry.options;

import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public enum InputFileFormat {

  text {

    /** Return a PTransform for decoding attribute-free PubsubMessages from payload strings. */
    public PTransform<PCollection<? extends String>, PCollection<PubsubMessage>> decode() {
      return DecodePubsubMessages.text();
    }
  },

  json {

    /** Return a PTransform for decoding PubsubMessages from JSON strings. */
    public PTransform<PCollection<? extends String>, PCollection<PubsubMessage>> decode() {
      return DecodePubsubMessages.json();
    }
  };

  public abstract PTransform<PCollection<? extends String>, PCollection<PubsubMessage>> decode();
}
