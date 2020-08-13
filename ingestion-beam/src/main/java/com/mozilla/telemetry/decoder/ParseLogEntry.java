package com.mozilla.telemetry.decoder;

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
      ObjectNode logEntry;
      HashMap<String, String> attributes = new HashMap<>();
      ObjectNode json = Json.createObjectNode();
      try {
        logEntry = Json.readObjectNode(m.getPayload());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      Optional.ofNullable(logEntry.path("insertId").textValue())
          .ifPresent(id -> attributes.put(Attribute.DOCUMENT_ID,
              UUID.nameUUIDFromBytes(id.getBytes(StandardCharsets.UTF_8)).toString()));
      Optional.ofNullable(logEntry.path("receiveTimestamp").textValue())
          .ifPresent(v -> attributes.put(Attribute.SUBMISSION_TIMESTAMP, v));
      Optional.ofNullable(logEntry.path("jsonPayload").path("Fields").path("userAgent").textValue())
          .ifPresent(v -> attributes.put(Attribute.USER_AGENT, v));
      String ecosystemAnonId = logEntry.path("jsonPayload").path("Fields").path("ecosystemAnonId")
          .textValue();
      String event = logEntry.path("jsonPayload").path("Fields").path("event").asText();
      // TODO: Parse geo info
      if ("oauth.token.created".equals(event) && ecosystemAnonId != null) {
        attributes.put(Attribute.URI,
            String.format("/submit/firefox-accounts/account-ecosystem/1/%s",
                attributes.get(Attribute.DOCUMENT_ID)));
        json.put("ecosystem_anon_id", ecosystemAnonId);
        return new PubsubMessage(Json.asBytes(json), attributes);
      } else {
        throw new IllegalArgumentException(
            "Received an unexpected payload without submission_timestamp");
      }
    }));
  }
}
