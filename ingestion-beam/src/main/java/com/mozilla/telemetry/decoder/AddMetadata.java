package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * A {@code PTransform} that adds metadata from attributes into the JSON payload.
 *
 * <p>This transform must come after {@code ParsePayload} to ensure any existing
 * "metadata" key in the payload has been removed. Otherwise, this transform could add a
 * duplicate key leading to invalid JSON.
 */
public class AddMetadata extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  public static final String METADATA = "metadata";

  private static final String GEO = "geo";
  private static final String GEO_PREFIX = GEO + "_";

  private static final String USER_AGENT_PREFIX = Attribute.USER_AGENT + "_";

  private static final String HEADER = "header";
  private static final List<String> HEADER_ATTRIBUTES = ImmutableList //
      .of(Attribute.DATE, Attribute.DNT, Attribute.X_PINGSENDER_VERSION, Attribute.X_DEBUG_ID);

  private static final List<String> URI_ATTRIBUTES = ImmutableList //
      .of(Attribute.URI, Attribute.APP_NAME, Attribute.APP_VERSION, Attribute.APP_UPDATE_CHANNEL,
          Attribute.APP_BUILD_ID);

  // These are identifying fields that we want to include in the payload so that the payload
  // can be replayed through Beam jobs even if PubSub attributes are lost; this stripping of
  // attributes occurs for BQ streaming insert errors. These fields are also useful when emitting
  // payload bytes to BQ.
  public static final List<String> IDENTIFYING_FIELDS = ImmutableList //
      .of(Attribute.DOCUMENT_NAMESPACE, Attribute.DOCUMENT_TYPE, Attribute.DOCUMENT_VERSION);

  private static final List<String> TOP_LEVEL_STRING_FIELDS = ImmutableList.of(
      Attribute.SUBMISSION_TIMESTAMP, Attribute.DOCUMENT_ID, //
      Attribute.NORMALIZED_APP_NAME, Attribute.NORMALIZED_CHANNEL, Attribute.NORMALIZED_OS,
      Attribute.NORMALIZED_OS_VERSION, Attribute.NORMALIZED_COUNTRY_CODE);

  private static final List<String> TOP_LEVEL_INT_FIELDS = ImmutableList.of(Attribute.SAMPLE_ID);

  public static AddMetadata of() {
    return INSTANCE;
  }

  /**
   * Return a map that includes a nested "metadata" map and various top-level metadata fields.
   *
   * <p>The structure of the metadata here should conform to the metadata/telemetry-ingestion or
   * metadata/structured-ingestion JSON schemas.
   * See https://github.com/mozilla-services/mozilla-pipeline-schemas
   */
  public static ObjectNode attributesToMetadataPayload(Map<String, String> attributes) {
    final String namespace = attributes.get(Attribute.DOCUMENT_NAMESPACE);
    // Currently, every entry in metadata is a Map<String, String>, but we keep Object as the
    // value type to support future evolution of the metadata structure to include fields that
    // are not specifically Map<String, String>.
    ObjectNode metadata = Json.createObjectNode();
    metadata.set(GEO, geoFromAttributes(attributes));
    metadata.set(Attribute.USER_AGENT, userAgentFromAttributes(attributes));
    metadata.set(HEADER, headersFromAttributes(attributes));
    if (ParseUri.TELEMETRY.equals(namespace)) {
      metadata.set(Attribute.URI, uriFromAttributes(attributes));
    }
    IDENTIFYING_FIELDS.forEach(name -> Optional //
        .ofNullable(attributes.get(name)) //
        .ifPresent(value -> metadata.put(name, value)));
    ObjectNode payload = Json.createObjectNode();
    payload.set(METADATA, metadata);
    TOP_LEVEL_STRING_FIELDS.forEach(name -> Optional //
        .ofNullable(attributes.get(name)) //
        .ifPresent(value -> payload.put(name, value)));
    TOP_LEVEL_INT_FIELDS.forEach(name -> Optional //
        .ofNullable(attributes.get(name)) //
        .flatMap(value -> {
          try {
            return Optional.of(Integer.parseInt(value));
          } catch (NumberFormatException e) {
            return Optional.empty();
          }
        }) //
        .ifPresent(value -> payload.put(name, value)));
    return payload;
  }

  /**
   * Called from {@link ParsePayload} where we have parsed the payload as {@link ObjectNode}
   * for validation, we strip out any existing metadata fields (which might be present if this
   * message went to error output and is now being reprocessed) to recover the original payload,
   * storing the metadata as PubSubMessage attributes.
   *
   * @param attributes the map into which we insert attributes
   * @param payload the json object from which we are stripping metadata fields
   */
  static void stripPayloadMetadataToAttributes(Map<String, String> attributes, ObjectNode payload) {
    Optional //
        .ofNullable(payload) //
        .map(p -> p.remove(METADATA)) //
        .filter(JsonNode::isObject) //
        .map(ObjectNode.class::cast) //
        .ifPresent(metadata -> {
          putGeoAttributes(attributes, metadata);
          putUserAgentAttributes(attributes, metadata);
          putHeaderAttributes(attributes, metadata);
          putUriAttributes(attributes, metadata);
          IDENTIFYING_FIELDS.forEach(name -> Optional.of(metadata) //
              .map(p -> metadata.remove(name)) //
              .filter(JsonNode::isTextual) //
              .ifPresent(value -> attributes.put(name, value.textValue())));
        });
    TOP_LEVEL_STRING_FIELDS.forEach(name -> Optional //
        .ofNullable(payload) //
        .map(p -> p.remove(name)) //
        .filter(JsonNode::isTextual) //
        .ifPresent(value -> attributes.put(name, value.textValue())));
    TOP_LEVEL_INT_FIELDS.forEach(name -> Optional //
        .ofNullable(payload) //
        .map(p -> p.remove(name)) //
        .ifPresent(value -> attributes.put(name, value.asText())));
  }

  static ObjectNode geoFromAttributes(Map<String, String> attributes) {
    ObjectNode geo = Json.createObjectNode();
    attributes.keySet().stream() //
        .filter(k -> k.startsWith(GEO_PREFIX)) //
        .forEach(k -> geo.put(k.substring(4), attributes.get(k)));
    return geo;
  }

  static void putGeoAttributes(Map<String, String> attributes, ObjectNode metadata) {
    putAttributes(attributes, metadata, GEO, GEO_PREFIX);
  }

  static ObjectNode userAgentFromAttributes(Map<String, String> attributes) {
    ObjectNode userAgent = Json.createObjectNode();
    attributes.entrySet().stream() //
        .filter(entry -> entry.getKey().startsWith(USER_AGENT_PREFIX)) //
        .forEach(entry -> userAgent.put(entry.getKey().substring(USER_AGENT_PREFIX.length()),
            entry.getValue()));
    return userAgent;
  }

  static void putUserAgentAttributes(Map<String, String> attributes, ObjectNode metadata) {
    putAttributes(attributes, metadata, Attribute.USER_AGENT, USER_AGENT_PREFIX);
  }

  static ObjectNode headersFromAttributes(Map<String, String> attributes) {
    ObjectNode header = Json.createObjectNode();
    attributes.entrySet().stream().filter(entry -> HEADER_ATTRIBUTES.contains(entry.getKey())) //
        .forEach(entry -> header.put(entry.getKey(), entry.getValue()));
    return header;
  }

  static void putHeaderAttributes(Map<String, String> attributes, ObjectNode metadata) {
    putAttributes(attributes, metadata, HEADER, "");
  }

  static ObjectNode uriFromAttributes(Map<String, String> attributes) {
    ObjectNode uri = Json.createObjectNode();
    attributes.entrySet().stream().filter(entry -> URI_ATTRIBUTES.contains(entry.getKey())) //
        .forEach(entry -> uri.put(entry.getKey(), entry.getValue()));
    return uri;
  }

  static void putUriAttributes(Map<String, String> attributes, ObjectNode metadata) {
    putAttributes(attributes, metadata, Attribute.URI, "");
  }

  @Override
  protected PubsubMessage processElement(PubsubMessage message) throws IOException {
    message = PubsubConstraints.ensureNonNull(message);
    // Get payload
    final byte[] payload = message.getPayload();
    // Get attributes as bytes, throws IOException
    final byte[] metadata = Json.asBytes(attributesToMetadataPayload(message.getAttributeMap()));
    // Ensure that we have a json object with no leading whitespace
    if (payload.length < 2 || payload[0] != '{') {
      throw new IOException("invalid json object: must start with {");
    }
    // Create an output stream for joining metadata with payload
    final ByteArrayOutputStream payloadWithMetadata = new ByteArrayOutputStream(
        metadata.length + payload.length);
    // Write metadata without trailing `}`
    payloadWithMetadata.write(metadata, 0, metadata.length - 1);
    // Start next json field, unless object was empty
    if (payload.length > 2) {
      // Write comma to start the next field
      payloadWithMetadata.write(',');
    }
    // Write payload without leading `{`
    payloadWithMetadata.write(payload, 1, payload.length - 1);
    return new PubsubMessage(payloadWithMetadata.toByteArray(), message.getAttributeMap());
  }

  ////////

  private static final AddMetadata INSTANCE = new AddMetadata();

  private AddMetadata() {
  }

  private static void putAttributes(Map<String, String> attributes, ObjectNode metadata,
      String nestingKey, String prefix) {
    Optional //
        .ofNullable(metadata) //
        .map(m -> m.path(nestingKey).fields()) //
        .map(Streams::stream).orElseGet(Stream::empty) //
        .filter(entry -> entry.getValue() != null) //
        .forEach(entry -> attributes.put(prefix + entry.getKey(), entry.getValue().asText()));
  }

}
