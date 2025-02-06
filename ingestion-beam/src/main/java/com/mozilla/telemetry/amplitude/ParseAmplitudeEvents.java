package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.Json;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Convert PubSub Message with event data to Amplitude event.
 */
public class ParseAmplitudeEvents extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<AmplitudeEvent>, PubsubMessage>> {

  private final String eventsAllowListPath;

  private static transient List<String[]> singletonAllowedEvents;

  public static ParseAmplitudeEvents of(String eventsAllowListPath) {
    return new ParseAmplitudeEvents(eventsAllowListPath);
  }

  private ParseAmplitudeEvents(String eventsAllowListPath) {
    this.eventsAllowListPath = eventsAllowListPath;
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

          try {
            readAllowedEventsFromFile();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

          final String namespace = Optional //
              .ofNullable(message.getAttribute(Attribute.DOCUMENT_NAMESPACE)) //
              .orElseThrow(() -> new InvalidAttributeException("Missing namespace"));
          final String docType = Optional //
              .ofNullable(message.getAttribute(Attribute.DOCUMENT_TYPE)) //
              .orElseThrow(() -> new InvalidAttributeException("Missing docType"));
          final String clientId = Optional //
              .ofNullable(message.getAttribute(Attribute.CLIENT_ID)) //
              .orElseThrow(() -> new InvalidAttributeException("Missing clientId"));
          final String appVersion = Optional //
              .ofNullable(message.getAttribute(Attribute.APP_VERSION)).orElseGet(() -> (null));

          try {
            final ArrayList<ObjectNode> events = extractEvents(payload, namespace, docType);

            return events.stream().map((ObjectNode event) -> {
              AmplitudeEvent.Builder amplitudeEventBuilder = AmplitudeEvent.builder();

              amplitudeEventBuilder.setTime(extractTimestamp(payload, event));
              amplitudeEventBuilder.setUserId(clientId);
              amplitudeEventBuilder.setAppVersion(appVersion);
              amplitudeEventBuilder.setEventType(event.get("event_type").asText());
              amplitudeEventBuilder.setEventExtras(event.get("event_extras").toString());

              return amplitudeEventBuilder.build();
            }).collect(Collectors.toList());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (UncheckedIOException | IOException | IllegalArgumentException
              | InvalidAttributeException e) {
            return FailureMessage.of(ParseAmplitudeEvents.class.getSimpleName(), ee.element(),
                ee.exception());
          }
        }));
  }

  @VisibleForTesting
  static long timestampStringToMillis(String timestamp) {
    return Instant.parse(timestamp).toEpochMilli();
  }

  @VisibleForTesting
  private static long extractTimestamp(ObjectNode payload, ObjectNode event) {
    JsonNode gleanTimestamp = event.path("event_extras").path(Attribute.GLEAN_TIMESTAMP);

    if (!gleanTimestamp.isMissingNode()) {
      return gleanTimestamp.asLong();
    }

    JsonNode startTime = payload.path(Attribute.PING_INFO).path(Attribute.PARSED_START_TIME);
    JsonNode eventTime = event.path(Attribute.TIMESTAMP);

    if (!startTime.isMissingNode() && !eventTime.isMissingNode()) {
      long timestampMillis = timestampStringToMillis(startTime.textValue());
      return timestampMillis + eventTime.asLong();
    }

    return Instant.now().toEpochMilli();
  }

  List<String[]> readAllowedEventsFromFile() throws IOException {
    if (singletonAllowedEvents != null) {
      return singletonAllowedEvents;
    }

    if (eventsAllowListPath == null) {
      throw new IllegalArgumentException("File location must be defined");
    }

    try (InputStream inputStream = BeamFileInputStream.open(eventsAllowListPath);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader)) {
      singletonAllowedEvents = new ArrayList<String[]>();

      while (reader.ready()) {
        String line = reader.readLine();

        if (line != null && !line.isEmpty()) {
          String[] separated = line.split(",");

          if (separated.length != 4) {
            throw new IllegalArgumentException(
                "Invalid mapping: " + line + "; four-column csv expected");
          }

          singletonAllowedEvents.add(separated);
        }
      }

      return singletonAllowedEvents;
    } catch (IOException e) {
      throw new IOException("Exception thrown while fetching " + eventsAllowListPath, e);
    }
  }

  @VisibleForTesting
  static ArrayList<ObjectNode> extractEvents(ObjectNode payload, String namespace, String docType)
      throws IOException {
    ArrayList<ObjectNode> events = new ArrayList<ObjectNode>();

    payload.path("events").forEach(event -> {
      final ObjectNode result = Json.createObjectNode();
      final JsonNode eventCategory = event.get("category");
      final JsonNode eventName = event.get("name");

      if (eventCategory != null && !eventCategory.isNull()) {
        String eventType = eventCategory.asText();

        if (eventName != null && !eventName.isNull()) {
          eventType = eventCategory.asText() + "." + eventName.asText();
        }

        if (singletonAllowedEvents.stream().anyMatch(c -> {
          if (c[0].equals(namespace) && c[1].equals(docType) && c[2].equals(eventCategory.asText())
              && (c[3].equals("*") || c[3].equals(eventName.asText()))) {
            return true;
          }

          return false;
        })) {
          ObjectMapper mapper = new ObjectMapper();
          result.put("event_type", eventType);
          result.put("event_extras", event.get("extra"));
          result.put("timestamp", event.get(Attribute.TIMESTAMP));
          events.add(result);
        }
      }
    });

    return events;
  }

  private static Optional<JsonNode> optionalNode(JsonNode... nodes) {
    return Stream.of(nodes).filter(n -> !n.isMissingNode()).findFirst();
  }
}
