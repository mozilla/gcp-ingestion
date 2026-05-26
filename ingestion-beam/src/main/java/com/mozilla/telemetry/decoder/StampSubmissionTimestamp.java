package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

/**
 * Stamps submission_timestamp on direct-Pub/Sub messages using Pub/Sub's publishTime.
 *
 * <p>Edge-published messages already carry submission_timestamp, so this short-circuits.
 * On reprocessing, the previously-stamped attribute survives and the short-circuit also
 * applies, preserving the original timestamp across republishes.
 */
public class StampSubmissionTimestamp
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static StampSubmissionTimestamp of() {
    return new StampSubmissionTimestamp();
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(ParDo.of(new Fn()));
  }

  static class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage message, @Timestamp Instant timestamp,
        OutputReceiver<PubsubMessage> out) {
      if (message.getAttribute(Attribute.SUBMISSION_TIMESTAMP) != null) {
        out.output(message);
        return;
      }
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
      attributes.put(Attribute.SUBMISSION_TIMESTAMP, timestamp.toString());
      out.output(new PubsubMessage(message.getPayload(), attributes));
    }
  }
}
