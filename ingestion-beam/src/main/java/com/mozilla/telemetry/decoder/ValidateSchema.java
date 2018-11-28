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
import org.apache.commons.text.StringSubstitutor;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;

public class ValidateSchema extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  private static class SchemaNotFoundException extends Exception {

    SchemaNotFoundException(String message) {
      super(message);
    }

    static SchemaNotFoundException forName(String name) {
      return new SchemaNotFoundException("No schema with name: " + name);
    }
  }

  private static final Map<String, Schema> schemas = new HashMap<>();

  @VisibleForTesting
  public static int numLoadedSchemas() {
    return schemas.size();
  }

  static {
    try {
      // Load all schemas from Java resources at classloading time so we can fail fast;
      // this means we'll be serializing the full HashMap of schemas when this class is sent
      // to workers, which isn't ideal, but it's also not so large that we need to be concerned.
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
    final Schema schema = schemas.get(name);
    if (schema == null) {
      throw SchemaNotFoundException.forName(name);
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
    try {

      // Throws IOException if not a valid json object
      final JSONObject json = Json.readJSONObject(element.getPayload());

      Map<String, String> attributes = element.getAttributeMap();
      if (attributes != null && !attributes.containsKey("document_version")) {
        // This element must be from the /submit/telemetry endpoint;
        // we need to version from the payload.
        attributes = new HashMap<>(attributes);
        try {
          String version = json.get("version").toString();
          attributes.put("document_version", version);
        } catch (JSONException e) {
          throw new SchemaNotFoundException("Element contains no document_version attribute, "
              + " so it's assumed to be a telemetry payload, but the payload does not include"
              + " the expected telemetry top-level 'version' field");
        }
      }

      // Throws SchemaNotFoundException if there's no schema
      final Schema schema = getSchema(attributes);
      // Throws ValidationException if schema doesn't match
      schema.validate(json);

      byte[] normalizedPayload = json.toString().getBytes();
      return new PubsubMessage(normalizedPayload, attributes);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
