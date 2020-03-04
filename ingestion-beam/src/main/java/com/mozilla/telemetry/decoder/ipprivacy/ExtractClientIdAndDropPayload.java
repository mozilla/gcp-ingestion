package com.mozilla.telemetry.decoder.ipprivacy;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
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

public class ExtractClientIdAndDropPayload extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  public static ExtractClientIdAndDropPayload of() {
    return new ExtractClientIdAndDropPayload();
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

          if (attributes.get(Attribute.CLIENT_ID) == null) {
            Optional.ofNullable(json.path("clientId").textValue())
                .map(ExtractClientIdAndDropPayload::normalizeUuid)
                .ifPresent(v -> attributes.put(Attribute.CLIENT_ID, v));
          }

          PerDocTypeCounter.inc(attributes, "valid_submission");
          PerDocTypeCounter.inc(attributes, "valid_submission_bytes", submissionBytes);

          return Collections
              .singletonList(new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributes));
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((WithFailures.ExceptionElement<PubsubMessage> e) -> FailureMessage
            .of(ExtractClientIdAndDropPayload.class.getSimpleName(), e.element(), e.exception())));
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
