/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.everit.json.schema.Schema;
import org.everit.json.schema.Validator;
import org.json.JSONObject;

public class JsonValidator {

  private final Validator validator;

  public JsonValidator() {
    // Without failEarly(), a pathological payload may cause the validator to consume all memory;
    // https://github.com/mozilla/gcp-ingestion/issues/374
    this.validator = Validator.builder().failEarly().build();
  }

  public void validate(Schema schema, ObjectNode json) throws JsonProcessingException {
    validator.performValidation(schema, Json.convertValue(json, JSONObject.class));
  }

}
