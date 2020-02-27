package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.mozilla.telemetry.decoder.MessageScrubber.MessageShouldBeDroppedException;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import com.mozilla.telemetry.ingestion.core.schema.SchemaNotFoundException;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.JsonValidator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;

/**
 * A {@code PTransform} that parses the message's payload as a JSON tree, sets
 * some attributes based on the content, and validates that it conforms to its schema.
 *
 * <p>There are several unrelated concerns all packed into this single transform so that we
 * incur the cost of parsing the JSON only once.
 */
public class ParsePayload extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  public static ParsePayload of(ValueProvider<String> schemasLocation) {
    return new ParsePayload(schemasLocation);
  }

  ////////

  private final Distribution parseTimer = Metrics.distribution(ParsePayload.class,
      "json_parse_millis");
  private final Distribution validateTimer = Metrics.distribution(ParsePayload.class,
      "json_validate_millis");

  private final ValueProvider<String> schemasLocation;

  private transient JsonValidator validator;
  private transient JSONSchemaStore schemaStore;
  private transient CRC32 crc32;

  private ParsePayload(ValueProvider<String> schemasLocation) {
    this.schemasLocation = schemasLocation;
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(FlatMapElements.into(TypeDescriptor.of(PubsubMessage.class)) //
        .via((PubsubMessage message) -> {
          message = PubsubConstraints.ensureNonNull(message);
          Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

          if (schemaStore == null) {
            schemaStore = JSONSchemaStore.of(schemasLocation.get(), BeamFileInputStream::open);
          }

          final int submissionBytes = message.getPayload().length;

          ObjectNode json;
          try {
            json = parseTimed(message.getPayload());
          } catch (IOException e) {
            Map<String, String> attrs = schemaStore.docTypeExists(message.getAttributeMap())
                ? message.getAttributeMap()
                : null; // null attributes will cause docType to show up as "unknown_doc_type"
            // in metrics
            PerDocTypeCounter.inc(attrs, "error_json_parse");
            PerDocTypeCounter.inc(attrs, "error_submission_bytes", submissionBytes);
            throw new RuntimeException(e);
          }

          // In case this message is being replayed from an error output where AddMetadata has
          // already
          // been applied, we strip out any existing metadata fields and put them into attributes.
          AddMetadata.stripPayloadMetadataToAttributes(attributes, json);

          // Check the contents of the message, potentially throwing an exception that causes the
          // message to be dropped or routed to error output; may also also alter the payload to
          // redact sensitive fields.
          try {
            MessageScrubber.scrub(attributes, json);
          } catch (MessageShouldBeDroppedException e) {
            // This message should go to no output, so we return an empty list immediately.
            return Collections.emptyList();
          }

          boolean validDocType = schemaStore.docTypeExists(attributes);
          if (!validDocType) {
            PerDocTypeCounter.inc(null, "error_invalid_doc_type");
            PerDocTypeCounter.inc(null, "error_submission_bytes", submissionBytes);
            throw new SchemaNotFoundException(String.format("No such docType: %s/%s",
                attributes.get("document_namespace"), attributes.get("document_type")));
          }

          // If no "document_version" attribute was parsed from the URI, this element must be from
          // the
          // /submit/telemetry endpoint and we now need to grab version from the payload.
          if (!attributes.containsKey("document_version")) {
            Optional<JsonNode> version = Stream.of(json.path("version"), json.path("v"))
                .filter(JsonNode::isValueNode).findFirst();
            if (version.isPresent()) {
              attributes.put(Attribute.DOCUMENT_VERSION, version.get().asText());
            } else {
              PerDocTypeCounter.inc(attributes, "error_missing_version");
              PerDocTypeCounter.inc(attributes, "error_submission_bytes", submissionBytes);
              throw new SchemaNotFoundException(
                  "Element was assumed to be a telemetry message because it contains no"
                      + " document_version attribute, but the payload does not include"
                      + " the top-level 'version' or 'v' field expected for a telemetry document");
            }
          }

          // Throws SchemaNotFoundException if there's no schema
          Schema schema;
          try {
            schema = schemaStore.getSchema(attributes);
          } catch (SchemaNotFoundException e) {
            PerDocTypeCounter.inc(attributes, "error_schema_not_found");
            PerDocTypeCounter.inc(attributes, "error_submission_bytes", submissionBytes);
            throw e;
          }

          try {
            validateTimed(schema, json);
          } catch (ValidationException e) {
            PerDocTypeCounter.inc(attributes, "error_schema_validation");
            PerDocTypeCounter.inc(attributes, "error_submission_bytes", submissionBytes);
            throw e;
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }

          addAttributesFromPayload(attributes, json);

          // https://github.com/mozilla/gcp-ingestion/issues/780
          // We need to be careful to consistently use our util methods (which use Jackson) for
          // serializing and de-serializing JSON to reduce the possibility of introducing encoding
          // issues. We previously called json.toString().getBytes() here without specifying a
          // charset.
          try {
            byte[] normalizedPayload = Json.asBytes(json);

            PerDocTypeCounter.inc(attributes, "valid_submission");
            PerDocTypeCounter.inc(attributes, "valid_submission_bytes", submissionBytes);

            return Collections.singletonList(new PubsubMessage(normalizedPayload, attributes));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).exceptionsInto(TypeDescriptor.of(PubsubMessage.class))
        .exceptionsVia((WithFailures.ExceptionElement<PubsubMessage> ee) -> FailureMessage.of(
            ParsePayload.class.getSimpleName(), //
            ee.element(), //
            ee.exception())));
  }

  private void addAttributesFromPayload(Map<String, String> attributes, ObjectNode json) {
    // Try to get glean-style client_info object.
    JsonNode gleanClientInfo = getGleanClientInfo(json);

    // Try to get "common ping"-style os object.
    JsonNode commonPingOs = json.path("environment").path("system").path("os");

    if (gleanClientInfo.isObject()) {
      // See glean ping structure in:
      // https://github.com/mozilla-services/mozilla-pipeline-schemas/blob/da4a1446efd948399eb9eade22f6fcbc5557f588/schemas/glean/baseline/baseline.1.schema.json
      Optional.ofNullable(gleanClientInfo.path("app_channel").textValue()) //
          .filter(v -> !Strings.isNullOrEmpty(v)) //
          .ifPresent(v -> attributes.put(Attribute.APP_UPDATE_CHANNEL, v));
      Optional.ofNullable(gleanClientInfo.path(Attribute.OS).textValue()) //
          .filter(v -> !Strings.isNullOrEmpty(v)) //
          .ifPresent(v -> attributes.put(Attribute.OS, v));
      Optional.ofNullable(gleanClientInfo.path(Attribute.OS_VERSION).textValue()) //
          .filter(v -> !Strings.isNullOrEmpty(v))
          .ifPresent(v -> attributes.put(Attribute.OS_VERSION, v));
    } else if (commonPingOs.isObject()) {
      // See common ping structure in:
      // https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/telemetry/data/common-ping.html
      Optional.ofNullable(commonPingOs.path("name").textValue()) //
          .filter(v -> !Strings.isNullOrEmpty(v)) //
          .ifPresent(v -> attributes.put(Attribute.OS, v));
      Optional.ofNullable(commonPingOs.path("version").textValue()) //
          .filter(v -> !Strings.isNullOrEmpty(v)) //
          .ifPresent(v -> attributes.put(Attribute.OS_VERSION, v));
    } else {
      // Try to extract "activity-stream"-style values.
      Optional.ofNullable(json.path("release_channel").textValue()) //
          .filter(v -> !Strings.isNullOrEmpty(v)) //
          .ifPresent(v -> attributes.put(Attribute.APP_UPDATE_CHANNEL, v));

      // Try to extract "core ping"-style values; see
      // https://github.com/mozilla-services/mozilla-pipeline-schemas/blob/da4a1446efd948399eb9eade22f6fcbc5557f588/schemas/telemetry/core/core.10.schema.json
      Optional.ofNullable(json.path(Attribute.OS).textValue()) //
          .filter(v -> !Strings.isNullOrEmpty(v)) //
          .ifPresent(v -> attributes.put(Attribute.OS, v));
      Optional.ofNullable(json.path("osversion")) //
          .map(JsonNode::textValue) //
          .filter(v -> !Strings.isNullOrEmpty(v)) //
          .ifPresent(v -> attributes.put(Attribute.OS_VERSION, v));
    }

    addClientIdFromPayload(attributes, json);

    // Add sample id, usually based on hashing clientId, but some other IDs are also supported to
    // allow sampling on non-telemetry pings.
    Stream.of(attributes.get(Attribute.CLIENT_ID),
        // "impression_id" is a client_id-like identifier used in activity-stream ping
        // that do not contain a client_id.
        json.path("impression_id").textValue()) //
        .map(ParsePayload::normalizeUuid) //
        .filter(Objects::nonNull) //
        .findFirst() //
        .ifPresent(v -> attributes.put(Attribute.SAMPLE_ID, Long.toString(calculateSampleId(v))));
  }

  /**
   * Extracts the client ID from the payload and adds it to `attributes`.
   *
   * <p>Side-effect: JSON payload is modified in this method. The client ID is normalized
   * in place in the JSON payload to avoid code duplication when extracting information from
   * different ping structures.
   */
  public static void addClientIdFromPayload(Map<String, String> attributes, ObjectNode json) {
    // Try to get glean-style client_info object.
    JsonNode gleanClientInfo = getGleanClientInfo(json);

    if (gleanClientInfo.isObject()) {
      // from glean ping
      Optional.ofNullable(gleanClientInfo.path(Attribute.CLIENT_ID).textValue()) //
          .filter(v -> !Strings.isNullOrEmpty(v)) //
          .map(ParsePayload::normalizeUuid) //
          .ifPresent(v -> {
            attributes.put(Attribute.CLIENT_ID, v);
            ((ObjectNode) gleanClientInfo).put(Attribute.CLIENT_ID, v);
          });
    }

    if (attributes.get(Attribute.CLIENT_ID) == null) {
      // from common ping
      Optional.ofNullable(json.path(Attribute.CLIENT_ID).textValue()) //
          .map(ParsePayload::normalizeUuid) //
          .ifPresent(v -> {
            attributes.put(Attribute.CLIENT_ID, v);
            json.put(Attribute.CLIENT_ID, v);
          });
    }

    if (attributes.get(Attribute.CLIENT_ID) == null) {
      Optional.ofNullable(json.path("clientId").textValue()) //
          .map(ParsePayload::normalizeUuid) //
          .ifPresent(v -> {
            attributes.put(Attribute.CLIENT_ID, v);
            json.put("clientId", v);
          });
    }
  }

  /** Tries to extract glean-style client_info object from payload. */
  private static JsonNode getGleanClientInfo(ObjectNode json) {
    return json.path("client_info");
  }

  @VisibleForTesting
  long calculateSampleId(String clientId) {
    if (crc32 == null) {
      crc32 = new CRC32();
    }
    crc32.reset();
    crc32.update(clientId.getBytes(StandardCharsets.UTF_8));
    return crc32.getValue() % 100;
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

  private void validateTimed(Schema schema, ObjectNode json) throws JsonProcessingException {
    if (validator == null) {
      validator = new JsonValidator();
    }
    long startTime = System.currentTimeMillis();
    validator.validate(schema, json);
    long endTime = System.currentTimeMillis();
    validateTimer.update(endTime - startTime);
  }
}
