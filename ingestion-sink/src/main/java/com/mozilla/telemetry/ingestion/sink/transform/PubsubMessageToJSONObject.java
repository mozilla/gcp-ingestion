/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.Constant;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Function;
import org.json.JSONObject;

/**
 * Transform a {@link PubsubMessage} into a {@code Map<String, Object>}.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class PubsubMessageToJSONObject implements Function<PubsubMessage, JSONObject> {

  public enum Format {
    raw, decoded, payload
  }

  private static final String PAYLOAD = "payload";
  private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

  private final Format format;

  public PubsubMessageToJSONObject(Format format) {
    this.format = format;
  }

  public JSONObject apply(PubsubMessage message) {
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
  private static JSONObject rawContents(PubsubMessage message) {
    JSONObject contents = new JSONObject(message.getAttributesMap());
    // bytes must be formatted as base64 encoded string.
    Optional.of(BASE64_ENCODER.encodeToString(message.getData().toByteArray()))
        // include payload if present.
        .filter(data -> !data.isEmpty()).ifPresent(data -> contents.put(PAYLOAD, data));
    return contents;
  }

  /**
   * Like {@link #rawContents(PubsubMessage)}, but uses the nested metadata format of decoded pings.
   */
  private static JSONObject decodedContents(PubsubMessage message) {
    JSONObject contents = new JSONObject(
        AddMetadata.attributesToMetadataPayload(message.getAttributesMap()));
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
