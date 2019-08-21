/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.cloud.bigquery.TableId;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.sink.io.BigQuery.Write.TableRow;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.text.StringSubstitutor;

public class PubsubMessageToTableRow implements Function<PubsubMessage, TableRow> {

  public enum TableRowFormat {
    raw, decoded, payload
  }

  public static final String PAYLOAD = "payload";

  private static final Base64.Encoder base64Encoder = Base64.getEncoder();

  private final String tableSpecTemplate;
  private final TableRowFormat tableRowFormat;

  public PubsubMessageToTableRow(String tableSpecTemplate, TableRowFormat tableRowFormat) {
    this.tableSpecTemplate = tableSpecTemplate;
    this.tableRowFormat = tableRowFormat;
  }

  private TableId getTableId(Map<String, String> attributes) {
    final String tableSpec = StringSubstitutor.replace(tableSpecTemplate, attributes);
    if (tableSpec.contains("$")) {
      throw new IllegalArgumentException("Element did not contain all the attributes needed to"
          + " fill out variables in the configured BigQuery output template: " + tableSpecTemplate);
    }
    final String[] tableSpecParts = tableSpec.split("\\.");
    if (tableSpecParts.length == 2) {
      return TableId.of(tableSpecParts[0], tableSpecParts[1]);
    } else if (tableSpecParts.length == 3) {
      return TableId.of(tableSpecParts[0], tableSpecParts[1], tableSpecParts[2]);
    } else {
      throw new IllegalArgumentException("Could not determine dataset from the configured BigQuery"
          + " output template: " + tableSpecTemplate);
    }
  }

  public TableRow apply(PubsubMessage message) {
    final TableId tableId = getTableId(message.getAttributesMap());
    final Optional<String> documentId = Optional
        .ofNullable(message.getAttributesOrDefault("document_id", null));
    final Map<String, Object> contents;
    switch (tableRowFormat) {
      case raw:
        contents = rawContents(message);
        break;
      case decoded:
        contents = decodedContents(message);
        break;
      case payload:
      default:
        throw new IllegalArgumentException(
            "TableRowFormat not yet implemented: " + tableRowFormat.name());
    }
    return new TableRow(tableId, message.getSerializedSize(), documentId, contents);
  }

  /**
   * Turn the message into a map that contains (likely gzipped) bytes as a "payload" field,
   * which is the format suitable for the "raw payload" tables in BigQuery that we use for
   * errors and recovery from pipeline failures.
   *
   * <p>We include all attributes as fields. It is up to the configured schema for the destination
   * table to determine which of those actually appear as fields; some of the attributes may be
   * thrown away.
   */
  private Map<String, Object> rawContents(PubsubMessage message) {
    Map<String, Object> contents = new HashMap<>(message.getAttributesMap());
    // bytes must be inserted as base64 encoded strings
    contents.put(PAYLOAD, base64Encoder.encodeToString(message.getData().toByteArray()));
    return contents;
  }

  /**
   * Like {@link #rawContents(PubsubMessage)}, but uses the nested metadata format of decoded pings.
   */
  static Map<String, Object> decodedContents(PubsubMessage message) {
    Map<String, Object> contents = AddMetadata
        .attributesToMetadataPayload(message.getAttributesMap());
    // bytes must be inserted as base64 encoded strings
    contents.put(PAYLOAD, base64Encoder.encodeToString(message.getData().toByteArray()));
    // Also include client_id if present.
    Optional.ofNullable(message.getAttributesOrDefault(Attribute.CLIENT_ID, null))
        .ifPresent(clientId -> contents.put(Attribute.CLIENT_ID, clientId));
    return contents;
  }
}
