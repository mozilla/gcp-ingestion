package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Extracts to attribute and scrubs IP addresses form Cloud Logging messages.
 *
 * <p>This is intended to be used with com.mozilla.telemetry.decoder.ParseLogEntry.
 * Messages sent from cloud logging contain IP addresses in their payload. For consistency with
 * the rest of the platform we are moving them to an attribute and removing from the payload,
 * so they don't make their way to error tables.
 */
public class ExtractIpFromLogEntry
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static ExtractIpFromLogEntry of() {
    return new ExtractIpFromLogEntry();
  }

  public class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    private final Counter countIpExtracted = Metrics.counter(Fn.class, "log_entry_ip_extracted");
    private final Counter countIpAlreadyExtracted = Metrics.counter(Fn.class,
        "log_entry_ip_already_extracted");

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      if (message.getAttribute(Attribute.SUBMISSION_TIMESTAMP) != null) {
        // If this message has submission_timestamp set, it is a normal message from the edge
        // server rather than a LogEntry from Cloud Logging, so we return immediately.
        return message;
      }

      if (message.getAttributeMap().containsKey(Attribute.X_FORWARDED_FOR)) {
        // Return early since IP has been extracted
        countIpAlreadyExtracted.inc();
        return message;
      }

      try {
        ObjectNode logEntry;
        final Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

        logEntry = Json.readObjectNode(message.getPayload());
        ObjectNode jsonPayload = (ObjectNode) logEntry.path("jsonPayload");
        ObjectNode fields = (ObjectNode) jsonPayload.path("Fields");
        JsonNode ipAddress = fields.remove("ip_address");
        if (ipAddress != null) {
          // if ipAddress is null, it means it wasn't present in the payload
          String clientIpAddress = ipAddress.textValue();
          attributes.put(Attribute.X_FORWARDED_FOR, clientIpAddress);
          countIpExtracted.inc();

          jsonPayload.replace("Fields", fields);
          logEntry.replace("jsonPayload", jsonPayload);
          byte[] sanitizedPayload = logEntry.toString().getBytes(StandardCharsets.UTF_8);

          return new PubsubMessage(sanitizedPayload, attributes);
        } else {
          return message;
        }

      } catch (IOException e) {
        throw new ParseLogEntry.InvalidLogEntryException(
            "Message has no submission_timestamp but could not be parsed as LogEntry JSON");
      }

    }
  }

  private final Fn fn = new Fn();

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }
}
