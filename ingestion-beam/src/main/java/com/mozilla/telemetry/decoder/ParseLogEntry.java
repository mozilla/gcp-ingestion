package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
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

      // Initialize attributes and payload.
      ObjectNode logEntry;
      HashMap<String, String> attributes = new HashMap<>();
      ObjectNode json = Json.createObjectNode();
      try {
        logEntry = Json.readObjectNode(m.getPayload());
      } catch (IOException e) {
        throw new InvalidLogEntryException(
            "Message has no submission_timestamp but could not be parsed as LogEntry JSON");
      }

      // Extract relevant data and attributes based on the LogEntry content.
      JsonNode fields = logEntry.path("jsonPayload").path("Fields");
      String documentId = Optional.ofNullable(logEntry.path("insertId").textValue())
          .map(id -> attributes.put(Attribute.DOCUMENT_ID,
              UUID.nameUUIDFromBytes(id.getBytes(StandardCharsets.UTF_8)).toString()))
          .orElse(UUID.randomUUID().toString());
      attributes.put(Attribute.DOCUMENT_ID, documentId);
      Optional.ofNullable(logEntry.path("receiveTimestamp").textValue())
          .ifPresent(v -> attributes.put(Attribute.SUBMISSION_TIMESTAMP, v));
      Optional.ofNullable(fields.path("userAgent").textValue())
          .ifPresent(v -> attributes.put(Attribute.USER_AGENT, v));
      String event = fields.path("event").textValue();
      String ecosystemAnonId = fields.path("ecosystemAnonId").textValue();
      attributes.put(Attribute.URI,
          String.format("/submit/firefox-accounts/account-ecosystem/1/%s", documentId));

      // Event-specific logic.
      if ("oauth.token.created".equals(event) && ecosystemAnonId != null) {
        json.put("ecosystem_anon_id", ecosystemAnonId);
      } else if ("account.updateEcosystemAnonId.complete".equals(event)) {
        Optional.ofNullable(logEntry.path("next").textValue())
            .ifPresent(v -> json.put("ecosystem_anon_id", v));
        Optional.ofNullable(logEntry.path("current").textValue())
            .ifPresent(v -> json.set("previous_ecosystem_anon_ids", Json.createArrayNode().add(v)));
      } else {
        throw new InvalidLogEntryException(
            "Message has no submission_timestamp but is not in a known LogEntry format");
      }

      // Add some additional fields if present.
      json.put("event", event);
      Optional.ofNullable(fields.path("country").textValue())
          .ifPresent(v -> json.put("country", v));
      Optional.ofNullable(fields.path("region").textValue()).ifPresent(v -> json.put("region", v));

      return new PubsubMessage(Json.asBytes(json), attributes);
    }).exceptionsInto(td).exceptionsVia(ee -> {
      try {
        throw ee.exception();
      } catch (InvalidLogEntryException e) {
        return FailureMessage.of(ParseLogEntry.class.getSimpleName(), ee.element(), ee.exception());
      }
    }));
  }
}
