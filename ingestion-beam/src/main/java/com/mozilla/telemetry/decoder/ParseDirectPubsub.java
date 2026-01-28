package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;

/**
 * Transforms messages published directly to Pub/Sub into a format compatible with structured
 * ingestion.
 *
 * <p>This is intended for consuming Glean server-side events delivered directly to Pub/Sub.
 * See TODO link
 * for glean_parser outputter used to generate client code for submitting these events.
 *
 * <p>Messages sent from the telemetry edge service are passed on without transformation.
 */
public class ParseDirectPubsub extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  public static ParseDirectPubsub of() {
    return new ParseDirectPubsub();
  }

  /**
   * Base class for all exceptions thrown by this class.
   */
  public static class InvalidDirectPubsubException extends RuntimeException {

    InvalidDirectPubsubException(String message) {
      super(message);
    }
  }

  final TupleTag<PubsubMessage> outputTag = new TupleTag<PubsubMessage>() {
  };
  final TupleTag<PubsubMessage> failureTag = new TupleTag<PubsubMessage>() {
  };

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> input) {
    PCollectionTuple parsed = input
        .apply(ParDo.of(new Fn()).withOutputTags(outputTag, TupleTagList.of(failureTag)));
    return WithFailures.Result.of(parsed.get(outputTag), parsed.get(failureTag));
  }

  class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    private final Counter countDirectPubsubPayload = Metrics.counter(Fn.class,
        "direct_pubsub_payload");

    @ProcessElement
    public void processElement(@Element PubsubMessage message, @Timestamp Instant timestamp,
        OutputReceiver<PubsubMessage> out, MultiOutputReceiver multiOut) {
      try {
        if (message.getAttribute(Attribute.SUBMISSION_TIMESTAMP) != null) {
          // If this message has submission_timestamp set, it is a normal message from the edge
          // server (or we're reprocessing it) rather than a direct Pub/Sub submission, so we pass
          // it through immediately.
          out.output(message);
          return;
        }
        countDirectPubsubPayload.inc();

        ObjectNode messagePayloadJson;
        try {
          messagePayloadJson = Json.readObjectNode(message.getPayload());
        } catch (IOException e) {
          throw new InvalidDirectPubsubException("Message payload could not be parsed as JSON");
        }

        // Extract required fields from ping envelope
        String documentId = messagePayloadJson.path("document_id").textValue();
        String documentNamespace = messagePayloadJson.path("document_namespace").textValue();
        String documentType = messagePayloadJson.path("document_type").textValue();
        String documentVersion = messagePayloadJson.path("document_version").textValue();
        String payload = messagePayloadJson.path("payload").textValue();

        boolean isValidDirectPubsub = documentId != null && documentNamespace != null
            && documentType != null && documentVersion != null && payload != null;
        if (!isValidDirectPubsub) {
          throw new InvalidDirectPubsubException(
              "Message is not in valid Direct Pub/Sub submission format");
        }

        Map<String, String> attributes = new HashMap<String, String>(message.getAttributeMap());
        attributes.put(Attribute.DOCUMENT_ID, documentId);
        attributes.put(Attribute.DOCUMENT_NAMESPACE, documentNamespace);
        attributes.put(Attribute.DOCUMENT_TYPE, documentType);
        attributes.put(Attribute.DOCUMENT_VERSION, documentVersion);

        // Use the Pub/Sub's publishTime as submission_timestamp
        String submissionTimestamp = timestamp.toString();
        attributes.put(Attribute.SUBMISSION_TIMESTAMP, submissionTimestamp);

        // Optionally extract user_agent if present
        String userAgent = messagePayloadJson.path("user_agent").textValue();
        if (userAgent != null) {
          attributes.put(Attribute.USER_AGENT, userAgent);
        }

        out.output(new PubsubMessage(payload.getBytes(StandardCharsets.UTF_8), attributes));
      } catch (InvalidDirectPubsubException e) {
        multiOut.get(failureTag)
            .output(FailureMessage.of(ParseDirectPubsub.class.getSimpleName(), message, e));
      }
    }
  }
}
