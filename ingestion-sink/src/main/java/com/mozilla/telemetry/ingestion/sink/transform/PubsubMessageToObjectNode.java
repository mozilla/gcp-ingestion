package com.mozilla.telemetry.ingestion.sink.transform;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.Constant;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Function;

/**
 * Transform a {@link PubsubMessage} into a {@link ObjectNode}.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class PubsubMessageToObjectNode implements Function<PubsubMessage, ObjectNode> {

  public enum Format {
    raw, decoded, payload
  }

  private static final String PAYLOAD = "payload";
  private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

  private final Format format;

  public PubsubMessageToObjectNode(Format format) {
    this.format = format;
  }

  @Override
  public ObjectNode apply(PubsubMessage message) {
    switch (format) {
      case raw:
        return rawContents(message);
      case decoded:
        return decodedContents(message);
      case payload:
      default:
        throw new IllegalArgumentException("Format not yet implemented: " + format.name());
    }
  }

  /**
   * Turn message into a map that contains (likely gzipped) bytes as a "payload" field,
   * which is the format suitable for the "raw payload" tables in BigQuery that we use for
   * errors and recovery from pipeline failures.
   *
   * <p>We include all attributes as fields. It is up to the configured schema for the destination
   * table to determine which of those actually appear as fields; some of the attributes may be
   * thrown away.
   */
  private static ObjectNode rawContents(PubsubMessage message) {
    ObjectNode contents = Json.asObjectNode(message.getAttributesMap());
    // bytes must be formatted as base64 encoded string.
    Optional.of(BASE64_ENCODER.encodeToString(message.getData().toByteArray()))
        // include payload if present.
        .filter(data -> !data.isEmpty()).ifPresent(data -> contents.put(PAYLOAD, data));
    return contents;
  }

  /**
   * Like {@link #rawContents(PubsubMessage)}, but uses the nested metadata format of decoded pings.
   *
   * <p>We include most attributes as fields, but the nested metadata format does not include error
   * attributes and only includes {@code metadata.uri} when document namespace is
   * {@code "telemetry"}.
   */
  private static ObjectNode decodedContents(PubsubMessage message) {
    ObjectNode contents = Json
        .asObjectNode(AddMetadata.attributesToMetadataPayload(message.getAttributesMap()));
    // bytes must be formatted as base64 encoded string.
    Optional.of(BASE64_ENCODER.encodeToString(message.getData().toByteArray()))
        // include payload if present.
        .filter(data -> !data.isEmpty()).ifPresent(data -> contents.put(PAYLOAD, data));
    // Also include client_id if present.
    Optional.ofNullable(message.getAttributesOrDefault(Constant.Attribute.CLIENT_ID, null))
        .ifPresent(clientId -> contents.put(Constant.Attribute.CLIENT_ID, clientId));
    return contents;
  }
}
