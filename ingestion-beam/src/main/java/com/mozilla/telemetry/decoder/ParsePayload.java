package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.mozilla.telemetry.decoder.MessageScrubber.MessageScrubberException;
import com.mozilla.telemetry.decoder.MessageScrubber.MessageShouldBeDroppedException;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore;
import com.mozilla.telemetry.ingestion.core.schema.SchemaNotFoundException;
import com.mozilla.telemetry.ingestion.core.transform.NestedMetadata;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.schema.SchemaStoreSingletonFactory;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.JsonValidator;
import com.mozilla.telemetry.util.Time;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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

  public static ParsePayload of(String schemasLocation) {
    return new ParsePayload(schemasLocation);
  }

  ////////

  private final Distribution parseTimer = Metrics.distribution(ParsePayload.class,
      "json_parse_millis");
  private final Distribution validateTimer = Metrics.distribution(ParsePayload.class,
      "json_validate_millis");

  private final String schemasLocation;

  private transient JsonValidator validator;
  private transient JSONSchemaStore schemaStore;
  private transient PipelineMetadataStore metadataStore;
  private transient CRC32 crc32;

  final TupleTag<PubsubMessage> outputTag = new TupleTag<>() {
  };
  final TupleTag<PubsubMessage> failureTag = new TupleTag<>() {
  };

  private ParsePayload(String schemasLocation) {
    this.schemasLocation = schemasLocation;
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    PCollectionTuple parsed = messages
        .apply(ParDo.of(new Fn()).withOutputTags(outputTag, TupleTagList.of(failureTag)));
    return WithFailures.Result.of(parsed.get(outputTag), parsed.get(failureTag));
  }

  private class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    @Setup
    public void setup() {
      schemaStore = SchemaStoreSingletonFactory.getJsonSchemaStore(schemasLocation);
      metadataStore = SchemaStoreSingletonFactory.getPipelineMetadataStore(schemasLocation);
      validator = new JsonValidator();
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage message, MultiOutputReceiver out) {
      try {
        message = PubsubConstraints.ensureNonNull(message);
        Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

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
          throw e;
        }

        // In case this message is being replayed from an error output where AddMetadata has already
        // been applied, we strip out any existing metadata fields and put them into attributes.
        NestedMetadata.stripPayloadMetadataToAttributes(attributes, json);

        // Check the contents of the message, potentially throwing an exception that causes the
        // message to be dropped or routed to error output; may also alter the payload to
        // redact sensitive fields.
        try {
          MessageScrubber.scrub(attributes, json);
        } catch (MessageShouldBeDroppedException e) {
          // This message should go to no output, so we return immediately without writing to output
          // receiver.
          return;
        }

        boolean validDocType = schemaStore.docTypeExists(attributes);
        if (!validDocType) {
          PerDocTypeCounter.inc(null, "error_invalid_doc_type");
          PerDocTypeCounter.inc(null, "error_submission_bytes", submissionBytes);
          throw new SchemaNotFoundException(String.format("No such docType: %s/%s",
              attributes.get("document_namespace"), attributes.get("document_type")));
        }

        // If no "document_version" attribute was parsed from the URI, this element must be from the
        // /submit/telemetry endpoint and we now need to grab version from the payload.
        if (!attributes.containsKey(Attribute.DOCUMENT_VERSION)) {
          try {
            String version = getVersionFromTelemetryPayload(json);
            attributes.put(Attribute.DOCUMENT_VERSION, version);
          } catch (SchemaNotFoundException e) {
            PerDocTypeCounter.inc(attributes, "error_missing_version");
            PerDocTypeCounter.inc(attributes, "error_submission_bytes", submissionBytes);
            throw e;
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
        }

        final PipelineMetadataStore.PipelineMetadata meta = metadataStore.getSchema(attributes);

        try {
          deprecationCheck(attributes, meta);
        } catch (DeprecatedMessageException e) {
          PerDocTypeCounter.inc(attributes, "error_deprecated_message");
          PerDocTypeCounter.inc(attributes, "error_submission_bytes", submissionBytes);
          throw e;
        }

        addAttributesFromPayload(attributes, json, meta);

        // https://github.com/mozilla/gcp-ingestion/issues/780
        // We need to be careful to consistently use our util methods (which use Jackson) for
        // serializing and de-serializing JSON to reduce the possibility of introducing encoding
        // issues. We previously called json.toString().getBytes() here without specifying a
        // charset.
        byte[] normalizedPayload = Json.asBytes(json);

        PerDocTypeCounter.inc(attributes, "valid_submission");
        PerDocTypeCounter.inc(attributes, "valid_submission_bytes", submissionBytes);

        out.get(outputTag).output(new PubsubMessage(normalizedPayload, attributes));
      } catch (IOException | SchemaNotFoundException | ValidationException
          | MessageScrubberException | DeprecatedMessageException e) {
        out.get(failureTag)
            .output(FailureMessage.of(ParsePayload.class.getSimpleName(), message, e));
      }
    }
  }

  /**
   * Return value of top-level "version" or "v" field in JSON payload.
   */
  public static String getVersionFromTelemetryPayload(JsonNode json) {
    Optional<JsonNode> version = Stream.of(json.path("version"), json.path("v"))
        .filter(JsonNode::isValueNode).findFirst();
    if (version.isPresent()) {
      return version.get().asText();
    } else {
      throw new SchemaNotFoundException(
          "Element was assumed to be a telemetry message because it contains no"
              + " document_version attribute, but the payload does not include"
              + " the top-level 'version' or 'v' field expected for a telemetry document");
    }
  }

  private void addAttributesFromPayload(Map<String, String> attributes, ObjectNode json,
      PipelineMetadataStore.PipelineMetadata meta) {
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

    // Add sample_id, by hashing an attribute or payload path that contains a UUID, ignore any
    // attribute or path that is not a valid UUID, and when both are available prefer the attribute
    String uuidAttribute = meta.sample_id_source_uuid_attribute();
    List<String> uuidPayloadPath = meta.sample_id_source_uuid_payload_path();
    if (uuidAttribute == null && uuidPayloadPath == null) {
      // default to using client_id
      uuidAttribute = Attribute.CLIENT_ID;
    }
    Stream.of(uuidAttribute == null ? null : attributes.get(uuidAttribute), //
        uuidPayloadPath == null ? null : jsonPathText(json, uuidPayloadPath)) //
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
  static JsonNode getGleanClientInfo(ObjectNode json) {
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
    long startTime = System.currentTimeMillis();
    validator.validate(schema, json);
    long endTime = System.currentTimeMillis();
    validateTimer.update(endTime - startTime);
  }

  private void deprecationCheck(Map<String, String> attributes,
      PipelineMetadataStore.PipelineMetadata meta) throws DeprecatedMessageException {
    if (meta.expiration_policy() != null
        && meta.expiration_policy().collect_through_date() != null) {

      LocalDate collectThroughDate;
      try {
        collectThroughDate = LocalDate.parse(meta.expiration_policy().collect_through_date(),
            DateTimeFormatter.ISO_LOCAL_DATE);
      } catch (DateTimeParseException e) {
        collectThroughDate = null;
      }

      Instant submissionInstant = Time
          .parseAsInstantOrNull(attributes.get(Attribute.SUBMISSION_TIMESTAMP));

      if (submissionInstant != null && collectThroughDate != null
          && LocalDate.ofInstant(submissionInstant, ZoneOffset.UTC).isAfter(collectThroughDate)) {
        throw new DeprecatedMessageException();
      }
    }
  }

  private static String jsonPathText(JsonNode json, List<String> path) {
    JsonNode node = json;
    for (String pathElement : path) {
      node = node.path(pathElement);
    }
    return node.asText();
  }

  static class DeprecatedMessageException extends RuntimeException {
  }
}
