/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.schemas.SchemaNotFoundException;
import com.mozilla.telemetry.schemas.SchemaStore;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.everit.json.schema.Schema;
import org.everit.json.schema.Validator;
import org.json.JSONObject;

/**
 * A {@code PTransform} that parses the message's payload as a {@link JSONObject}, sets
 * some attributes based on the content, and validates that it conforms to its schema.
 *
 * <p>There are several unrelated concerns all packed into this single transform so that we
 * incur the cost of parsing the JSON only once.
 */
public class ParsePayload extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  public static ParsePayload of(ValueProvider<String> schemasLocation) {
    return new ParsePayload(schemasLocation);
  }

  ////////

  private final Distribution parseTimer = Metrics.distribution(ParsePayload.class,
      "json_parse_millis");
  private final Distribution validateTimer = Metrics.distribution(ParsePayload.class,
      "json_validate_millis");
  private final Counter invalidDocTypeCounter = Metrics.counter(ParsePayload.class,
      "invalid_doc_type");

  private final ValueProvider<String> schemasLocation;

  private transient Validator validator;
  private transient SchemaStore schemaStore;

  private ParsePayload(ValueProvider<String> schemasLocation) {
    this.schemasLocation = schemasLocation;
  }

  @Override
  protected PubsubMessage processElement(PubsubMessage message)
      throws SchemaNotFoundException, IOException {
    message = PubsubConstraints.ensureNonNull(message);

    // Throws IOException if not a valid json object
    final JSONObject json = parseTimed(message.getPayload());

    Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

    // Remove any top-level "metadata" field if it exists, and attempt to parse it as a
    // key-value map of strings, adding all entries as attributes.
    Object untypedMetadata = json.remove("metadata");
    if (untypedMetadata instanceof JSONObject) {
      JSONObject metadata = (JSONObject) untypedMetadata;
      for (String key : metadata.keySet()) {
        Object value = metadata.get(key);
        if (value != null) {
          attributes.put(key, metadata.get(key).toString());
        }
      }
    }

    if (schemaStore == null) {
      schemaStore = SchemaStore.of(schemasLocation);
    }
    if (!schemaStore.docTypeExists(attributes)) {
      invalidDocTypeCounter.inc();
      throw new SchemaNotFoundException(String.format("No such docType: %s/%s",
          attributes.get("document_namespace"), attributes.get("document_type")));
    }

    // If no "document_version" attribute was parsed from the URI, this element must be from the
    // /submit/telemetry endpoint and we now need to grab version from the payload.
    if (!attributes.containsKey("document_version")) {
      if (json.has("version")) {
        String version = json.get("version").toString();
        attributes.put("document_version", version);
      } else if (json.has("v")) {
        String version = json.get("v").toString();
        attributes.put("document_version", version);
      } else {
        throw new SchemaNotFoundException("Element was assumed to be a telemetry message because"
            + " it contains no document_version attribute, but the payload does not include"
            + " the top-level 'version' or 'v' field expected for a telemetry document");
      }
    }

    // Throws SchemaNotFoundException if there's no schema
    final Schema schema = schemaStore.getSchema(attributes);
    // Throws ValidationException if schema doesn't match
    validateTimed(schema, json);

    byte[] normalizedPayload = json.toString().getBytes();
    return new PubsubMessage(normalizedPayload, attributes);
  }

  private JSONObject parseTimed(byte[] bytes) throws IOException {
    long startTime = System.currentTimeMillis();
    final JSONObject json = Json.readJSONObject(bytes);
    long endTime = System.currentTimeMillis();
    parseTimer.update(endTime - startTime);
    return json;
  }

  private void validateTimed(Schema schema, JSONObject json) {
    if (validator == null) {
      // Without failEarly(), a pathological payload may cause the validator to consume all memory;
      // https://github.com/mozilla/gcp-ingestion/issues/374
      validator = Validator.builder().failEarly().build();
    }
    long startTime = System.currentTimeMillis();
    validator.performValidation(schema, json);
    long endTime = System.currentTimeMillis();
    validateTimer.update(endTime - startTime);
  }
}
