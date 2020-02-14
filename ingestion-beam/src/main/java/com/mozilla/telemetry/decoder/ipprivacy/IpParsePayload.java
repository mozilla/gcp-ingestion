package com.mozilla.telemetry.decoder.ipprivacy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.mozilla.telemetry.decoder.MessageScrubber;
import com.mozilla.telemetry.decoder.ParsePayload;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class IpParsePayload extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  public static IpParsePayload of() {
    return new IpParsePayload();
  }

  ////////

  private final Distribution parseTimer = Metrics.distribution(ParsePayload.class,
      "json_parse_millis");

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(FlatMapElements.into(TypeDescriptor.of(PubsubMessage.class))
        .via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);
          Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

          final int submissionBytes = message.getPayload().length;

          ObjectNode json;
          try {
            json = parseTimed(message.getPayload());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }

          // Check the contents of the message, potentially throwing an exception that causes the
          // message to be dropped or routed to error output; may also also alter the payload to
          // redact sensitive fields.
          try {
            MessageScrubber.scrub(attributes, json);
          } catch (RuntimeException e) {
            // This message should go to no output, so we return an empty list immediately.
            return Collections.emptyList();
          }

          addClientIdFromPayload(attributes, json);

          PerDocTypeCounter.inc(attributes, "valid_submission");
          PerDocTypeCounter.inc(attributes, "valid_submission_bytes", submissionBytes);

          return Collections
              .singletonList(new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributes));
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((WithFailures.ExceptionElement<PubsubMessage> e) -> FailureMessage
            .of(IpParsePayload.class.getSimpleName(), e.element(), e.exception())));
  }

  private void addClientIdFromPayload(Map<String, String> attributes, ObjectNode json) {

    // Try to get glean-style client_info object.
    JsonNode gleanClientInfo = json.path("client_info");

    if (gleanClientInfo.isObject()) {
      // See glean ping structure in:
      // https://github.com/mozilla-services/mozilla-pipeline-schemas/blob/da4a1446efd948399eb9eade22f6fcbc5557f588/schemas/glean/baseline/baseline.1.schema.json
      Optional.ofNullable(gleanClientInfo.path(Attribute.CLIENT_ID).textValue()) //
          .filter(v -> !Strings.isNullOrEmpty(v)) //
          .map(IpParsePayload::normalizeUuid) //
          .ifPresent(v -> {
            attributes.put(Attribute.CLIENT_ID, v);
          });
    }

    if (attributes.get(Attribute.CLIENT_ID) == null) {
      Optional.ofNullable(json.path(Attribute.CLIENT_ID).textValue()) //
          .map(IpParsePayload::normalizeUuid) //
          .ifPresent(v -> {
            attributes.put(Attribute.CLIENT_ID, v);
          });
    }

    if (attributes.get(Attribute.CLIENT_ID) == null) {
      Optional.ofNullable(json.path("clientId").textValue()) //
          .map(IpParsePayload::normalizeUuid) //
          .ifPresent(v -> {
            attributes.put(Attribute.CLIENT_ID, v);
          });
    }
  }

  @VisibleForTesting
  static String normalizeUuid(String v) {
    if (v == null) {
      return null;
    }
    // The impression_id in activity-stream pings is a UUID enclosed in curly braces, so we
    v = v.replaceAll("[{}]", "");
    try {
      // Will raise an exception if not a valid UUID.
      UUID.fromString(v);
      return v.toLowerCase();
    } catch (IllegalArgumentException ignore) {
      return null;
    }
  }

  private ObjectNode parseTimed(byte[] bytes) throws IOException {
    long startTime = System.currentTimeMillis();
    final ObjectNode json = Json.readObjectNode(bytes);
    long endTime = System.currentTimeMillis();
    parseTimer.update(endTime - startTime);
    return json;
  }
}
