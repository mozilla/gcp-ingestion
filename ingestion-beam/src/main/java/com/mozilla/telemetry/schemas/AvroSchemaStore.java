/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.schemas;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.compress.archivers.ArchiveInputStream;

public class AvroSchemaStore extends SchemaStore<Schema> {

  /** Returns a SchemaStore based on the contents of the archive at schemasLocation. */
  public static AvroSchemaStore of(ValueProvider<String> schemasLocation) {
    return new AvroSchemaStore(schemasLocation);
  }

  protected AvroSchemaStore(ValueProvider<String> schemasLocation) {
    super(schemasLocation, null);
  }

  @Override
  protected boolean containsSchemaSuffix(String name) {
    return name.endsWith(".avro.json");
  }

  @Override
  protected Schema loadSchemaFromArchive(ArchiveInputStream archive) throws IOException {
    Schema.Parser parser = new Schema.Parser();
    return parser.parse(archive);
  }
}
