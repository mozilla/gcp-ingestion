/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.io.Resources;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.commons.text.StringSubstitutor;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

public class ValidateSchema extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {
  private static class SchemaNotFoundException extends Exception {
    SchemaNotFoundException(String msg) {
      super(msg);
    }
  }

  private static final Map<String, Schema> schemas = new HashMap<>();

  private static void loadSchema(String name) throws SchemaNotFoundException {
    try {
      final URL url = Resources.getResource(name);
      final byte[] schema = Resources.toByteArray(url);
      // Throws IOException if not a valid json object
      JSONObject rawSchema = Json.readJSONObject(schema);
      schemas.put(name, SchemaLoader.load(rawSchema));
    } catch (IOException e) {
      // TODO load all existing schemas on startup
      throw new SchemaNotFoundException("No schema with name:" + name);
    }
  }

  private static Schema getSchema(Map<String, String> attributes) throws SchemaNotFoundException {
    if (attributes == null) {
      throw new SchemaNotFoundException("No schema for message with null attributeMap");
    }
    // This is the path provided by mozilla-pipeline-schemas
    final String name = StringSubstitutor.replace(
        "schemas/${document_namespace}/${document_type}/"
            + "${document_type}.${document_version}.schema.json",
        attributes);
    if (!schemas.containsKey(name)) {
      loadSchema(name);
    }
    return schemas.get(name);
  }

  @Override
  protected PubsubMessage processElement(PubsubMessage element)
      throws SchemaNotFoundException, IOException {
    // Throws IOException if not a valid json object
    final JSONObject json = Json.readJSONObject(element.getPayload());
    // Throws SchemaNotFoundException if there's no schema
    final Schema schema = getSchema(element.getAttributeMap());
    // Throws ValidationException if schema doesn't match
    schema.validate(json);
    return new PubsubMessage(json.toString().getBytes(), element.getAttributeMap());
  }
}
