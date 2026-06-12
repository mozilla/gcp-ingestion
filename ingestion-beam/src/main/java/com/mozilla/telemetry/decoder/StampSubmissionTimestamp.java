package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableList;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;

/**
 * Prepares direct-Pub/Sub messages for the main decoder stage:
 * stamps {@code submission_timestamp} from Pub/Sub's publishTime and rejects
 * messages missing any of the metadata attributes the publisher is expected to set
 * ({@code document_id}, {@code document_namespace}, {@code document_type},
 * {@code document_version}).
 *
 * <p>Edge-published messages already carry {@code submission_timestamp} and stamping
 * short-circuits in that case. On reprocessing, the previously-stamped attribute survives
 * and the short-circuit also applies, preserving the original timestamp across republishes.
 *
 * <p>This mirrors the validation that {@link ParseLogEntry} applies for log-ingestion input,
 * so both alternative ingestion paths fail with a clear, transform-specific error rather than
 * a downstream {@code SchemaNotFoundException} (or, for {@code document_id}, silent success).
 */
public class StampSubmissionTimestamp extends
    PTransform<PCollection<PubsubMessage>, WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage>> {

  public static StampSubmissionTimestamp of() {
    return new StampSubmissionTimestamp();
  }

  /** Thrown when a direct-Pub/Sub message is missing a required attribute. */
  public static class MissingAttributeException extends RuntimeException {

    MissingAttributeException(String attribute) {
      super("Direct-Pub/Sub message is missing required attribute: " + attribute);
    }
  }

  private static final List<String> REQUIRED_ATTRIBUTES = ImmutableList.of(
      Attribute.DOCUMENT_NAMESPACE, Attribute.DOCUMENT_TYPE, Attribute.DOCUMENT_VERSION,
      Attribute.DOCUMENT_ID);

  final TupleTag<PubsubMessage> outputTag = new TupleTag<>() {
  };
  final TupleTag<PubsubMessage> failureTag = new TupleTag<>() {
  };

  @Override
  public WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> input) {
    PCollectionTuple tuple = input
        .apply(ParDo.of(new Fn()).withOutputTags(outputTag, TupleTagList.of(failureTag)));
    return WithFailures.Result.of(tuple.get(outputTag), tuple.get(failureTag));
  }

  class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage message, @Timestamp Instant timestamp,
        MultiOutputReceiver out) {
      try {
        for (String attribute : REQUIRED_ATTRIBUTES) {
          String value = message.getAttribute(attribute);
          if (value == null || value.isEmpty()) {
            throw new MissingAttributeException(attribute);
          }
        }

        if (message.getAttribute(Attribute.SUBMISSION_TIMESTAMP) != null) {
          out.get(outputTag).output(message);
          return;
        }
        Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
        attributes.put(Attribute.SUBMISSION_TIMESTAMP, timestamp.toString());
        out.get(outputTag).output(new PubsubMessage(message.getPayload(), attributes));
      } catch (MissingAttributeException e) {
        out.get(failureTag)
            .output(FailureMessage.of(StampSubmissionTimestamp.class.getSimpleName(), message, e));
      }
    }
  }
}
