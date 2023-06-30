package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Transforms messages from Cloud Logging into a format compatible with structured ingestion.
 *
 * <p>Messages sent from the telemetry edge service are passed on without transformation.
 */
public class ParseLogEntry extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final Counter countLogEntryPayload = Metrics.counter(ParseLogEntry.class,
      "log_entry_payload");

  public static ParseLogEntry of() {
    return new ParseLogEntry();
  }

  /**
   * Base class for all exceptions thrown by this class.
   */
  public static class InvalidLogEntryException extends RuntimeException {

    InvalidLogEntryException(String message) {
      super(message);
    }
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> input) {
    TypeDescriptor<PubsubMessage> td = TypeDescriptor.of(PubsubMessage.class);
    return input.apply(MapElements.into(td).via((PubsubMessage m) -> {
      if (m.getAttribute(Attribute.SUBMISSION_TIMESTAMP) != null) {
        // If this message has submission_timestamp set, it is a normal message from the edge
        // server rather than a LogEntry from Cloud Logging, so we return immediately.
        return m;
      }
      countLogEntryPayload.inc();

      ObjectNode logEntry;
      HashMap<String, String> attributes = new HashMap<>();
      try {
        logEntry = Json.readObjectNode(m.getPayload());
      } catch (IOException e) {
        throw new InvalidLogEntryException(
            "Message has no submission_timestamp but could not be parsed as LogEntry JSON");
      }

      // Extract relevant data and attributes based on the LogEntry content.
      JsonNode fields = logEntry.path("jsonPayload").path("Fields");

      String documentId = fields.path("document_id").textValue();
      String documentNamespace = fields.path("document_namespace").textValue();
      String documentType = fields.path("document_type").textValue();
      String documentVersion = fields.path("document_version").textValue();
      String payload = fields.path("payload").textValue();
      String receiveTimestamp = logEntry.path("receiveTimestamp").textValue();

      boolean isValidLogEntry = documentId != null && documentNamespace != null
          && documentType != null && documentVersion != null && payload != null
          && receiveTimestamp != null;
      if (!isValidLogEntry) {
        throw new InvalidLogEntryException(
            "Message has no submission_timestamp but is not in a known LogEntry format");
      }

      attributes.put(Attribute.DOCUMENT_ID, documentId);
      attributes.put(Attribute.SUBMISSION_TIMESTAMP, receiveTimestamp);
      attributes.put(Attribute.URI, String.format("/submit/%s/%s/%s/%s", documentNamespace,
          documentType, documentVersion, documentId));

      return new PubsubMessage(payload.getBytes(StandardCharsets.UTF_8), attributes);
    }).exceptionsInto(td).exceptionsVia(ee -> {
      try {
        throw ee.exception();
      } catch (InvalidLogEntryException e) {
        return FailureMessage.of(ParseLogEntry.class.getSimpleName(), ee.element(), ee.exception());
      }
    }));
  }
}
