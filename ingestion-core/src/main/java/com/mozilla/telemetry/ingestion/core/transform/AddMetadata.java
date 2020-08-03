package com.mozilla.telemetry.ingestion.core.transform;

import com.google.common.collect.ImmutableList;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@code PTransform} that adds metadata from attributes into the JSON payload.
 *
 * <p>This transform must come after {@code ParsePayload} to ensure any existing
 * "metadata" key in the payload has been removed. Otherwise, this transform could add a
 * duplicate key leading to invalid JSON.
 */
public class AddMetadata {

  private AddMetadata() {
  }

  public static final String METADATA = "metadata";

  private static final String GEO = "geo";
  private static final String GEO_PREFIX = GEO + "_";

  private static final String ISP = "isp";
  private static final String ISP_PREFIX = ISP + "_";

  private static final String USER_AGENT_PREFIX = Attribute.USER_AGENT + "_";

  private static final String HEADER = "header";
  private static final List<String> HEADER_ATTRIBUTES = ImmutableList //
      .of(Attribute.DATE, Attribute.DNT, Attribute.X_DEBUG_ID, Attribute.X_PINGSENDER_VERSION,
          Attribute.X_SOURCE_TAGS);

  private static final List<String> URI_ATTRIBUTES = ImmutableList //
      .of(Attribute.URI, Attribute.APP_NAME, Attribute.APP_VERSION, Attribute.APP_UPDATE_CHANNEL,
          Attribute.APP_BUILD_ID);

  // These are identifying fields that we want to include in the payload so that the payload
  // can be replayed through Beam jobs even if PubSub attributes are lost; this stripping of
  // attributes occurs for BQ streaming insert errors. These fields are also useful when emitting
  // payload bytes to BQ.
  private static final List<String> IDENTIFYING_FIELDS = ImmutableList //
      .of(Attribute.DOCUMENT_NAMESPACE, Attribute.DOCUMENT_TYPE, Attribute.DOCUMENT_VERSION);

  private static final List<String> TOP_LEVEL_STRING_FIELDS = ImmutableList.of(
      Attribute.SUBMISSION_TIMESTAMP, Attribute.DOCUMENT_ID, //
      Attribute.NORMALIZED_APP_NAME, Attribute.NORMALIZED_CHANNEL, Attribute.NORMALIZED_OS,
      Attribute.NORMALIZED_OS_VERSION, Attribute.NORMALIZED_COUNTRY_CODE);

  private static final List<String> TOP_LEVEL_INT_FIELDS = ImmutableList.of(Attribute.SAMPLE_ID);

  /**
   * Return a map that includes a nested "metadata" map and various top-level metadata fields.
   *
   * <p>The structure of the metadata here should conform to the metadata/telemetry-ingestion or
   * metadata/structured-ingestion JSON schemas.
   * See https://github.com/mozilla-services/mozilla-pipeline-schemas
   */
  public static Map<String, Object> attributesToMetadataPayload(Map<String, String> attributes) {
    final String namespace = attributes.get(Attribute.DOCUMENT_NAMESPACE);
    // Currently, every entry in metadata is a Map<String, String>, but we keep Object as the
    // value type to support future evolution of the metadata structure to include fields that
    // are not specifically Map<String, String>.
    Map<String, Object> metadata = new HashMap<>();
    metadata.put(GEO, geoFromAttributes(attributes));
    metadata.put(ISP, ispFromAttributes(attributes));
    metadata.put(Attribute.USER_AGENT, userAgentFromAttributes(attributes));
    metadata.put(HEADER, headersFromAttributes(attributes));
    if ("telemetry".equals(namespace)) {
      metadata.put(Attribute.URI, uriFromAttributes(attributes));
    }
    IDENTIFYING_FIELDS.forEach(name -> Optional //
        .ofNullable(attributes.get(name)) //
        .ifPresent(value -> metadata.put(name, value)));
    Map<String, Object> payload = new HashMap<>();
    payload.put(METADATA, metadata);
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

  private static Map<String, Object> geoFromAttributes(Map<String, String> attributes) {
    HashMap<String, Object> geo = new HashMap<>();
    attributes.keySet().stream() //
        .filter(k -> k.startsWith(GEO_PREFIX)) //
        .forEach(k -> geo.put(k.substring(4), attributes.get(k)));
    return geo;
  }

  private static Map<String, Object> ispFromAttributes(Map<String, String> attributes) {
    HashMap<String, Object> isp = new HashMap<>();
    attributes.keySet().stream() //
        .filter(k -> k.startsWith(ISP_PREFIX)) //
        .forEach(k -> isp.put(k.substring(4), attributes.get(k)));
    return isp;
  }

  private static Map<String, Object> userAgentFromAttributes(Map<String, String> attributes) {
    HashMap<String, Object> userAgent = new HashMap<>();
    attributes.keySet().stream() //
        .filter(k -> k.startsWith(USER_AGENT_PREFIX)) //
        .forEach(k -> userAgent.put(k.substring(11), attributes.get(k)));
    return userAgent;
  }

  private static Map<String, Object> headersFromAttributes(Map<String, String> attributes) {
    HashMap<String, Object> header = new HashMap<>();
    HEADER_ATTRIBUTES.stream().forEach(name -> Optional //
        .ofNullable(attributes.get(name)) //
        .ifPresent(value -> header.put(name, value)));
    return header;
  }

  private static Map<String, Object> uriFromAttributes(Map<String, String> attributes) {
    HashMap<String, Object> uri = new HashMap<>();
    URI_ATTRIBUTES.stream().forEach(name -> Optional //
        .ofNullable(attributes.get(name)) //
        .ifPresent(value -> uri.put(name, value)));
    return uri;
  }
}
