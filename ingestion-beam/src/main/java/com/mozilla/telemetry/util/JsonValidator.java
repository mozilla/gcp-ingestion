package com.mozilla.telemetry.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.Validator;
import org.json.JSONArray;
import org.json.JSONObject;

public class JsonValidator {

  private final Validator validator;

  /** Build a JSON validator object.*/
  public JsonValidator() {
    // Without failEarly(), a pathological payload may cause the validator to consume all memory;
    // https://github.com/mozilla/gcp-ingestion/issues/374
    this.validator = Validator.builder().failEarly().build();
  }

  public void validate(Schema schema, ObjectNode json) throws JsonProcessingException {
    validator.performValidation(schema, Json.convertValue(json, JSONObject.class));
  }

  /**
   * Validate a Jackson array.
   *
   * <p>Note this requires serializing the array back to a JSON string because
   * the Jackson mapper cannot convert an ArrayNode to an org.json.JSONArray
   * type.
   */
  public void validate(Schema schema, ArrayNode json) throws JsonProcessingException, IOException {
    InputStream serialized = IOUtils.toInputStream(json.toString(), "UTF-8");
    JSONArray array = Json.readJsonArray(serialized);
    validator.performValidation(schema, array);
  }
}
