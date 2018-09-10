/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.io.Resources;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.utils.Json;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.text.StringSubstitutor;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

public class ValidateSchema
    extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

  public static TupleTag<PubsubMessage> mainTag = new TupleTag<PubsubMessage>();
  public static TupleTag<PubsubMessage> errorTag = new TupleTag<PubsubMessage>();

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

  private static PubsubMessage transform(PubsubMessage element)
      throws SchemaNotFoundException, IOException {
    // Throws IOException if not a valid json object
    final JSONObject json = Json.readJSONObject(element.getPayload());
    // Throws SchemaNotFoundException if there's no schema
    final Schema schema = getSchema(element.getAttributeMap());
    // Throws ValidationException if schema doesn't match
    schema.validate(json);
    return new PubsubMessage(json.toString().getBytes(), element.getAttributeMap());
  }

  private static class Fn extends DoFn<PubsubMessage, PubsubMessage> {
    @ProcessElement
    public void processElement(@Element PubsubMessage element, MultiOutputReceiver out) {
      try {
        out.get(mainTag).output(transform(element));
      } catch (Throwable e) {
        out.get(errorTag).output(
            FailureMessage.of(this, element, e));
      }
    }
  }

  private static final Fn FN = new Fn();

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {
    PCollectionTuple output = input.apply(ParDo
        .of(FN)
        .withOutputTags(mainTag, TupleTagList.of(errorTag))
    );
    output.get(errorTag).setCoder(PubsubMessageWithAttributesCoder.of());
    return output;
  }
}
