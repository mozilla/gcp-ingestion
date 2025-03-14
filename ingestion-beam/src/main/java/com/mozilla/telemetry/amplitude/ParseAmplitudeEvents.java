package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
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

  private static transient volatile List<String[]> singletonAllowedEvents;

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

          // A CSV file contains a set of event names and categories that should be sent to
          // Amplitude.
          // This is to limit the event volume and cost in Amplitude.
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
          final Integer sampleId = Optional //
              .ofNullable(Integer.parseInt(message.getAttribute(Attribute.SAMPLE_ID))) //
              .orElseThrow(() -> new InvalidAttributeException("Missing sampleId"));
          final String appVersion = Optional //
              .ofNullable(message.getAttribute(Attribute.APP_VERSION)).orElseGet(() -> (null));
          final String osName = Optional //
              .ofNullable(message.getAttribute(Attribute.NORMALIZED_OS)).orElseGet(() -> (null));
          final String osVersion = Optional //
              .ofNullable(message.getAttribute(Attribute.NORMALIZED_OS_VERSION))
              .orElseGet(() -> (null));
          final String country = Optional //
              .ofNullable(message.getAttribute(Attribute.GEO_COUNTRY)).orElseGet(() -> (null));
          final String device_model = Optional //
              .ofNullable(message.getAttribute(Attribute.DEVICE_MODEL)).orElseGet(() -> (null));
          final String device_manufacturer = Optional //
              .ofNullable(message.getAttribute(Attribute.DEVICE_MANUFACTURER))
              .orElseGet(() -> (null));
          final String locale = Optional //
              .ofNullable(message.getAttribute(Attribute.LOCALE)).orElseGet(() -> (null));

          final JsonNode experimentsNode = payload.path(Attribute.PING_INFO)
              .path(Attribute.EXPERIMENTS);
          final Map<String, String> experiments = new HashMap<>();
          if (!experimentsNode.isMissingNode()) {
            Iterator<Map.Entry<String, JsonNode>> fields = experimentsNode.fields();
            while (fields.hasNext()) {
              Map.Entry<String, JsonNode> field = fields.next();
              String experiment = field.getKey();
              String branch = field.getValue().get("branch").asText();
              experiments.put(experiment, branch);
            }
          }

          try {
            if (docType.equals("metrics")) {
              // For the POC, metric pings are used to indicate whether a client was active on a
              // given day.
              // This is not a 100% accurate representation of user activity, but close enough for
              // the POC.
              // See https://mozilla-hub.atlassian.net/browse/DENG-7616
              AmplitudeEvent.Builder userActivityEventBuilder = AmplitudeEvent.builder();

              userActivityEventBuilder.setTime(Instant.now().toEpochMilli());
              userActivityEventBuilder.setUserId(clientId);
              userActivityEventBuilder.setSampleId(sampleId);
              userActivityEventBuilder.setAppVersion(appVersion);
              userActivityEventBuilder.setPlatform(namespace);
              userActivityEventBuilder.setOsName(osName);
              userActivityEventBuilder.setOsVersion(osVersion);
              userActivityEventBuilder.setCountry(country);
              userActivityEventBuilder.setDeviceModel(device_model);
              userActivityEventBuilder.setDeviceManufacturer(device_manufacturer);
              userActivityEventBuilder.setLanguage(locale);
              userActivityEventBuilder.setExperiments(experiments);
              userActivityEventBuilder.setEventType("user_activity");

              return ImmutableList.of(userActivityEventBuilder.build());
            } else {
              // each event from the payload is mapped to a separate Amplitude event
              final List<ObjectNode> events = extractEvents(payload, namespace, docType);
              return events.stream().map((ObjectNode event) -> {
                AmplitudeEvent.Builder amplitudeEventBuilder = AmplitudeEvent.builder();

                amplitudeEventBuilder.setTime(extractTimestamp(payload, event));
                amplitudeEventBuilder.setUserId(clientId);
                amplitudeEventBuilder.setSampleId(sampleId);
                amplitudeEventBuilder.setAppVersion(appVersion);
                amplitudeEventBuilder.setPlatform(namespace);
                amplitudeEventBuilder.setOsName(osName);
                amplitudeEventBuilder.setOsVersion(osVersion);
                amplitudeEventBuilder.setCountry(country);
                amplitudeEventBuilder.setDeviceModel(device_model);
                amplitudeEventBuilder.setDeviceManufacturer(device_manufacturer);
                amplitudeEventBuilder.setLanguage(locale);
                amplitudeEventBuilder.setExperiments(experiments);
                amplitudeEventBuilder.setEventType(event.get("event_type").asText());
                amplitudeEventBuilder.setEventExtras(event.get("event_extras").toString());

                return amplitudeEventBuilder.build();
              }).collect(Collectors.toList());
            }
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

  /**
   * Compute the event timestamp.
   * Based on the same logic used in:
   * https://github.com/mozilla/bigquery-etl/blob/cb1f059c1ba3437747baced399b09b3b89725024/sql_generators/glean_usage/templates/events_stream_v1.query.sql#L127-L130
   */
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

  /**
   * Reads allowed document name, document type, event category and event name combination from CSV.
   * Expected format:
   * org-mozilla-ios-firefox,events,bookmark,*
   * org-mozilla-fenix,events,nimbus_events,enroll_failed
   */
  List<String[]> readAllowedEventsFromFile() throws IOException {
    if (singletonAllowedEvents != null) {
      return singletonAllowedEvents;
    }

    if (eventsAllowListPath == null) {
      throw new IllegalArgumentException("File location must be defined");
    }

    synchronized (ParseAmplitudeEvents.class) {
      if (singletonAllowedEvents == null) {
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
        } catch (IOException e) {
          throw new IOException("Exception thrown while fetching " + eventsAllowListPath, e);
        }
      }
    }

    return singletonAllowedEvents;
  }

  /**
   * Read events from ping payload, filter based on allowed events and return as JSON.
   */
  @VisibleForTesting
  static List<ObjectNode> extractEvents(ObjectNode payload, String namespace, String docType)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    return StreamSupport.stream(payload.path("events").spliterator(), false).map(event -> {
      final ObjectNode result = mapper.createObjectNode();
      final JsonNode eventCategory = event.get("category");
      final JsonNode eventName = event.get("name");
      String eventType = "";

      if (eventCategory != null && !eventCategory.isNull()) {
        eventType = eventCategory.asText();

        if (eventName != null && !eventName.isNull()) {
          eventType += "." + eventName.asText();
        }
      }

      if (isEventAllowed(namespace, docType, eventCategory, eventName)) {
        result.put("event_type", eventType);
        result.put("event_extras", event.get("extra"));
        result.put("timestamp", event.get(Attribute.TIMESTAMP));
        return result;
      }
      return null;
    }).filter(result -> result != null).collect(Collectors.toList());
  }

  static boolean isEventAllowed(String namespace, String docType, JsonNode eventCategory,
      JsonNode eventName) {
    return singletonAllowedEvents.stream()
        .anyMatch(c -> c[0].equals(namespace) && c[1].equals(docType)
            && (c[2].equals(eventCategory.asText()) || c[2].equals("*"))
            && (c[3].equals("*") || c[3].equals(eventName.asText())));
  }

  private static Optional<JsonNode> optionalNode(JsonNode... nodes) {
    return Stream.of(nodes).filter(n -> !n.isMissingNode()).findFirst();
  }
}
