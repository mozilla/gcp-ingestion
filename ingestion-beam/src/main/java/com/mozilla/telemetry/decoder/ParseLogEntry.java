package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Transforms messages from Cloud Logging into a format compatible with structured ingestion.
 *
 * <p>Messages sent from the telemetry edge service are passed on without transformation.
 */
public class ParseLogEntry
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  private final Counter countLogEntryPayload = Metrics.counter(ParseLogEntry.class,
      "log_entry_payload");

  public static ParseLogEntry of() {
    return new ParseLogEntry();
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via(m -> {
      if (m.getAttribute(Attribute.SUBMISSION_TIMESTAMP) != null) {
        // If this message has submission_timestamp set, it is a normal message from the edge
        // server rather than a LogEntry from Cloud Logging, so we return immediately.
        return m;
      }
      countLogEntryPayload.inc();
      ObjectNode logEntry;
      HashMap<String, String> attributes = new HashMap<>();
      ObjectNode json = Json.createObjectNode();
      try {
        logEntry = Json.readObjectNode(m.getPayload());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
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
      String event = fields.path("event").asText();
      String ecosystemAnonId = fields.path("ecosystemAnonId").textValue();
      attributes.put(Attribute.URI,
          String.format("/submit/firefox-accounts/account-ecosystem/1/%s", documentId));
      // TODO: Parse geo info
      if ("oauth.token.created".equals(event) && ecosystemAnonId != null) {
        json.put("ecosystem_anon_id", ecosystemAnonId);
      } else if ("account.updateEcosystemAnonId.complete".equals(event)) {
        Optional.ofNullable(logEntry.path("next").textValue())
            .ifPresent(v -> json.put("ecosystem_anon_id", v));
        Optional.ofNullable(logEntry.path("current").textValue())
            .ifPresent(v -> json.set("previous_ecosystem_anon_ids", Json.createArrayNode().add(v)));
      } else {
        throw new IllegalArgumentException(
            "Received an unexpected payload without submission_timestamp");
      }
      json.put("event", event);
      return new PubsubMessage(Json.asBytes(json), attributes);
    }));
  }
}
