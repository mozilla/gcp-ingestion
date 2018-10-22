/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

public enum SeenMessagesSource {

  pubsub {

    /** Read from a PubSub subscription. */
    public PCollection<PubsubMessage> read(DecoderOptions.Parsed options,
        PCollection<PubsubMessage> undelivered) {
      return undelivered.getPipeline().apply(PubsubIO.readMessagesWithAttributes()
          .fromSubscription(options.getDeliveredMessagesSubscription()));
    }
  },

  undelivered {

    /** Mark messages as seen without waiting for delivery. */
    public PCollection<PubsubMessage> read(DecoderOptions.Parsed options,
        PCollection<PubsubMessage> undelivered) {
      return undelivered;
    }
  },

  none {

    /** Do not mark messages as seen. */
    public PCollection<PubsubMessage> read(DecoderOptions.Parsed options,
        PCollection<PubsubMessage> undelivered) {
      return undelivered.getPipeline().apply("create empty collection",
          Create.empty(PubsubMessageWithAttributesCoder.of()));
    }
  };

  /** Get a PCollection of messages to mark as seen from a PCollection of undelivered messages. */
  public abstract PCollection<PubsubMessage> read(DecoderOptions.Parsed options,
      PCollection<PubsubMessage> undelivered);
}
