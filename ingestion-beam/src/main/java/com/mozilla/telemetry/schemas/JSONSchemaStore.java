/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.schemas;

import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JSONSchemaStore extends SchemaStore<Schema> {

  /** Returns a SchemaStore based on the contents of the archive at schemasLocation. */
  public static JSONSchemaStore of(ValueProvider<String> schemasLocation) {
    return new JSONSchemaStore(schemasLocation);
  }

  protected JSONSchemaStore(ValueProvider<String> schemasLocation) {
    super(schemasLocation);
  }

  @Override
  protected boolean containsSchemaSuffix(String name) {
    return name.endsWith(".schema.json");
  }

  @Override
  protected Schema loadSchemaFromArchive(ArchiveInputStream archive) throws IOException {
    byte[] bytes = IOUtils.toByteArray(archive);
    JSONObject json = Json.readJSONObject(bytes);
    return SchemaLoader.load(json);
  }
}
