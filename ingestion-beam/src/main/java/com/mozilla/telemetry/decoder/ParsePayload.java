/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import nl.basjes.shaded.org.springframework.core.io.Resource;
import nl.basjes.shaded.org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.commons.text.StringSubstitutor;
import org.everit.json.schema.Schema;
import org.everit.json.schema.Validator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

/**
 * A {@code PTransform} that parses the message's payload as a {@link JSONObject}, sets
 * some attributes based on the content, and validates that it conforms to its schema.
 *
 * <p>There are several unrelated concerns all packed into this single transform so that we
 * incur the cost of parsing the JSON only once.
 */
public class ParsePayload extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  public static ParsePayload of() {
    return INSTANCE;
  }

  ////////

  private final Distribution parseTimer = Metrics.distribution(ParsePayload.class,
      "json-parse-millis");
  private final Distribution validateTimer = Metrics.distribution(ParsePayload.class,
      "json-validate-millis");

  private transient Validator validator;

  private static class SchemaNotFoundException extends Exception {

    SchemaNotFoundException(String message) {
      super(message);
    }

    static SchemaNotFoundException forName(String name) {
      return new SchemaNotFoundException("No schema with name: " + name);
    }
  }

  private static final ParsePayload INSTANCE = new ParsePayload();
  private static final Map<String, Schema> schemas = new HashMap<>();

  private ParsePayload() {
  }

  @VisibleForTesting
  public static int numLoadedSchemas() {
    return schemas.size();
  }

  static {
    try {
      // Load all schemas from Java resources at classloading time so we can fail fast in tests,
      // but this pre-loaded state won't be available to workers since PTransforms are only
      // pseudo-serializable and state outside the DoFn won't get sent.
      loadAllSchemas();
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error while loading JSON schemas", e);
    }
  }

  private static void loadAllSchemas() throws SchemaNotFoundException, IOException {
    final Resource[] resources = new PathMatchingResourcePatternResolver()
        .getResources("classpath*:schemas/**/*.schema.json");
    for (Resource resource : resources) {
      final String name = resource.getURL().getPath().replaceFirst("^.*/schemas/", "schemas/");
      loadSchema(name);
    }
  }

  private static void loadSchema(String name) throws SchemaNotFoundException {
    try {
      final URL url = Resources.getResource(name);
      final byte[] schema = Resources.toByteArray(url);
      // Throws IOException if not a valid json object
      JSONObject rawSchema = Json.readJSONObject(schema);
      schemas.put(name, SchemaLoader.load(rawSchema));
    } catch (IOException e) {
      throw SchemaNotFoundException.forName(name);
    }
  }

  private static Schema getSchema(String name) throws SchemaNotFoundException {
    Schema schema = schemas.get(name);
    if (schema == null) {
      loadSchema(name);
      schema = schemas.get(name);
    }
    return schema;
  }

  private static Schema getSchema(Map<String, String> attributes) throws SchemaNotFoundException {
    if (attributes == null) {
      throw new SchemaNotFoundException("No schema for message with null attributeMap");
    }
    // This is the path provided by mozilla-pipeline-schemas
    final String name = StringSubstitutor.replace("schemas/${document_namespace}/${document_type}/"
        + "${document_type}.${document_version}.schema.json", attributes);
    return getSchema(name);
  }

  @Override
  protected PubsubMessage processElement(PubsubMessage element)
      throws SchemaNotFoundException, IOException {
    // Throws IOException if not a valid json object
    final JSONObject json = parseTimed(element.getPayload());

    Map<String, String> attributes;
    if (element.getAttributeMap() == null) {
      attributes = new HashMap<>();
    } else {
      attributes = new HashMap<>(element.getAttributeMap());
    }

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
    final Schema schema = getSchema(attributes);
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
