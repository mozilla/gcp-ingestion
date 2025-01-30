package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Convert PubSub Message with event data to Amplitude event.
 */
public class ParseAmplitudeEvents extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<AmplitudeEvent>, PubsubMessage>> {

  public static ParseAmplitudeEvents of() {
    return new ParseAmplitudeEvents();
  }

  private ParseAmplitudeEvents() {
  }

  @Override
  public Result<PCollection<AmplitudeEvent>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(FlatMapElements.into(TypeDescriptor.of(AmplitudeEvent.class))
        .via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);

          ObjectNode payload;
          try {
            payload = Json.readObjectNode(message.getPayload());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

          String namespace = Optional //
              .ofNullable(message.getAttribute(Attribute.DOCUMENT_NAMESPACE)) //
              .orElseThrow(() -> new InvalidAttributeException("Missing namespace"));
          String docType = Optional //
              .ofNullable(message.getAttribute(Attribute.DOCUMENT_TYPE)) //
              .orElseThrow(() -> new InvalidAttributeException("Missing docType"));
          long submissionTimestamp = Optional //
              .ofNullable(timstampStringToMillis(message.getAttribute(Attribute.SUBMISSION_TIMESTAMP))) //
              .orElseGet(() -> (Instant.now().toEpochMilli()));
          String clientId = Optional //
              .ofNullable(message.getAttribute(Attribute.CLIENT_ID)) //
              .orElseThrow(() -> new InvalidAttributeException("Missing clientId"));
          String appVersion = Optional //
              .ofNullable(message.getAttribute(Attribute.APP_VERSION)) //
              .orElseThrow(() -> new InvalidAttributeException("Missing appVersion"));


            ArrayList<ObjectNode> events = extractEvents(payload);

            return events.stream().map((ObjectNode event) -> {
              AmplitudeEvent.Builder amplitudeEventBuilder = AmplitudeEvent.builder();

              amplitudeEventBuilder.setTime(submissionTimestamp);
              amplitudeEventBuilder.setUserId(clientId);
              amplitudeEventBuilder.setAppVersion(appVersion);
              amplitudeEventBuilder.setEventType(event.get("event_type").toString());
              amplitudeEventBuilder.setEventProperties(event.get("event_properties"));
              // todo: handle event timestamp

              return amplitudeEventBuilder.build();
            }).collect(Collectors.toList());


        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (UncheckedIOException | IOException | IllegalArgumentException | InvalidAttributeException e) {
            return FailureMessage.of(ParseAmplitudeEvents.class.getSimpleName(), ee.element(),
                ee.exception());
          }
        }));
  }

  @VisibleForTesting
  static long timstampStringToMillis(String timestamp) {
    return Instant.parse(timestamp).toEpochMilli();
  }

    /**
   *
   * @throws IOException
   */
  @VisibleForTesting
  static ArrayList<ObjectNode> extractEvents(ObjectNode payload) {
    ArrayList<ObjectNode> events = new ArrayList<ObjectNode>();

    payload.path("events").forEach(event -> {
      final ObjectNode result = Json.createObjectNode();
      JsonNode eventCategory = event.get("category");
      JsonNode eventName = event.get("name");

      System.err.println(Json.asString(event));

      System.err.println(Json.asString(eventName));

      if (eventCategory != null) {
        String eventType = eventCategory.asText();

        if (eventName != null) {
          eventType = eventCategory.asText() + "." + eventName.asText();
        }

        ObjectMapper mapper = new ObjectMapper();
        result.put("event_type", eventType);
        result.put("event_properties", result.get("extra"));
        events.add(result);
      }
    });

    return events;
  }
}
