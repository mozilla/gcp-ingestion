/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * A {@code PTransform} that adds metadata from attributes into the JSON payload.
 *
 * <p>This transform must come after {@code ParsePayload} to ensure any existing
 * "metadata" key in the payload has been removed. Otherwise, this transform could add a
 * duplicate key leading to invalid JSON.
 */
public class AddMetadata extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  private static final String METADATA = "metadata";

  private static final String GEO = "geo";
  private static final String GEO_PREFIX = GEO + "_";

  private static final String USER_AGENT = "user_agent";
  private static final String USER_AGENT_PREFIX = USER_AGENT + "_";

  private static final String HEADER = "header";
  private static final List<String> HEADER_ATTRIBUTES = ImmutableList //
      .of("date", "dnt", "x_pingsender_version", "x_debug_id");

  private static final String URI = "uri";
  private static final List<String> URI_ATTRIBUTES = ImmutableList //
      .of("app_name", "app_version", "app_update_channel", "app_build_id");

  private static final List<String> TOP_LEVEL_STRING_FIELDS = ImmutableList.of(
      "submission_timestamp", "document_id", //
      "normalized_app_name", "normalized_channel", //
      "normalized_os", "normalized_os_version", //
      "normalized_country_code");

  private static final List<String> TOP_LEVEL_INT_FIELDS = ImmutableList.of("sample_id");

  public static AddMetadata of() {
    return INSTANCE;
  }

  static Map<String, Object> attributesToMetadataPayload(Map<String, String> attributes) {
    final String namespace = attributes.get("document_namespace");
    // Currently, every entry in metadata is a Map<String, String>, but we keep Object as the
    // value type to support future evolution of the metadata structure to include fields that
    // are not specifically Map<String, String>.
    Map<String, Object> metadata = new HashMap<>();
    metadata.put(GEO, geoFromAttributes(attributes));
    metadata.put(USER_AGENT, userAgentFromAttributes(attributes));
    metadata.put(HEADER, headersFromAttributes(attributes));
    if ("telemetry".equals(namespace)) {
      metadata.put(URI, uriFromAttributes(attributes));
    }
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

  static void stripPayloadMetadataToAttributes(Map<String, String> attributes,
      Map<String, Object> payload) {
    Optional.ofNullable(payload) //
        .map(p -> p.remove(METADATA)).filter(Map.class::isInstance) //
        .ifPresent(m -> {
          Map<String, Object> metadata = (Map<String, Object>) m;
          putGeoAttributes(attributes, metadata);
          putUserAgentAttributes(attributes, metadata);
          putHeaderAttributes(attributes, metadata);
          putUriAttributes(attributes, metadata);
        });
    TOP_LEVEL_STRING_FIELDS.forEach(name -> Optional //
        .ofNullable(payload) //
        .map(p -> p.remove(name)) //
        .filter(String.class::isInstance) //
        .ifPresent(value -> attributes.put(name, value.toString())));
    TOP_LEVEL_INT_FIELDS.forEach(name -> Optional //
        .ofNullable(payload) //
        .map(p -> p.remove(name)) //
        .filter(Integer.class::isInstance) //
        .ifPresent(value -> attributes.put(name, value.toString())));
  }

  static Map<String, Object> geoFromAttributes(Map<String, String> attributes) {
    HashMap<String, Object> geo = new HashMap<>();
    attributes.keySet().stream() //
        .filter(k -> k.startsWith(GEO_PREFIX)) //
        .forEach(k -> geo.put(k.substring(4), attributes.get(k)));
    return geo;
  }

  static void putGeoAttributes(Map<String, String> attributes, Map<String, Object> metadata) {
    Optional.ofNullable(metadata).map(m -> m.get(GEO)) //
        .filter(Map.class::isInstance) //
        .map(m -> ((Map<String, Object>) m).entrySet()) //
        .orElse(ImmutableSet.of()) //
        .forEach(entry -> attributes.put(GEO_PREFIX + entry.getKey(), entry.getValue().toString()));
  }

  static Map<String, Object> userAgentFromAttributes(Map<String, String> attributes) {
    HashMap<String, Object> userAgent = new HashMap<>();
    attributes.keySet().stream() //
        .filter(k -> k.startsWith(USER_AGENT_PREFIX)) //
        .forEach(k -> userAgent.put(k.substring(11), attributes.get(k)));
    return userAgent;
  }

  static void putUserAgentAttributes(Map<String, String> attributes, Map<String, Object> metadata) {
    Optional.ofNullable(metadata).map(m -> m.get(USER_AGENT)) //
        .filter(Map.class::isInstance) //
        .map(m -> ((Map<String, Object>) m).entrySet()) //
        .orElse(ImmutableSet.of()) //
        .forEach(entry -> attributes.put(USER_AGENT_PREFIX + entry.getKey(),
            entry.getValue().toString()));
  }

  static Map<String, Object> headersFromAttributes(Map<String, String> attributes) {
    HashMap<String, Object> header = new HashMap<>();
    HEADER_ATTRIBUTES.stream().forEach(name -> Optional //
        .ofNullable(attributes.get(name)) //
        .ifPresent(value -> header.put(name, value)));
    return header;
  }

  static void putHeaderAttributes(Map<String, String> attributes, Map<String, Object> metadata) {
    Optional.ofNullable(metadata).map(m -> m.get(HEADER)) //
        .filter(Map.class::isInstance) //
        .map(m -> ((Map<String, Object>) m).entrySet()) //
        .orElse(ImmutableSet.of()) //
        .forEach(entry -> attributes.put(entry.getKey(), entry.getValue().toString()));
  }

  static Map<String, Object> uriFromAttributes(Map<String, String> attributes) {
    HashMap<String, Object> uri = new HashMap<>();
    URI_ATTRIBUTES.stream().forEach(name -> Optional //
        .ofNullable(attributes.get(name)) //
        .ifPresent(value -> uri.put(name, value)));
    return uri;
  }

  static void putUriAttributes(Map<String, String> attributes, Map<String, Object> metadata) {
    Optional.ofNullable(metadata).map(m -> m.get(URI)) //
        .filter(Map.class::isInstance) //
        .map(m -> ((Map<String, Object>) m).entrySet()) //
        .orElse(ImmutableSet.of()) //
        .forEach(entry -> attributes.put(entry.getKey(), entry.getValue().toString()));
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

}
