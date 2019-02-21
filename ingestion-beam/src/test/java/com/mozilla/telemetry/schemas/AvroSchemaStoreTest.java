/* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.schemas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;
import org.junit.Before;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;

public class AvroSchemaStoreTest {

  /**
    avro-schema-test/
  └── schemas
      ├── namespace_0
      │   ├── bar
      │   │   └── bar.1.avro.json
      │   └── foo
      │       └── foo.1.avro.json
      └── namespace_1
          └── baz
              ├── baz.1.avro.json
              └── baz.1.schema.json

  6 directories, 4 files
  */
  private File schemaFile = null;

  @Before
  public void setup() throws IOException {
    byte[] data = "{\"type\": \"null\"}".getBytes();
    String[] paths = { "/schemas/namespace_0/foo/foo.1.avro.json",
        "/schemas/namespace_0/bar/bar.1.avro.json", "/schemas/namespace_1/baz/baz.1.avro.json",
        "/schemas/namespace_1/baz/baz.1.schema.json", };

    schemaFile = File.createTempFile("avro", ".schemas.tar.gz");
    schemaFile.deleteOnExit();

    try (OutputStream fos = new FileOutputStream(schemaFile);
        OutputStream gzo = new GzipCompressorOutputStream(fos);
        TarArchiveOutputStream taos = new TarArchiveOutputStream(gzo);) {
      for (String path : paths) {
        TarArchiveEntry entry = new TarArchiveEntry(path);
        entry.setSize(data.length);
        taos.putArchiveEntry(entry);
        taos.write(data);
        taos.closeArchiveEntry();
        temp.delete();
      }
    }
  }

  @Test
  public void testNumSchemas() {
    ValueProvider<String> location = StaticValueProvider.of(schemaFile.getPath());
    AvroSchemaStore store = AvroSchemaStore.of(location);
    assertEquals(store.numLoadedSchemas(), 3);
  }

  @Test
  public void testDocTypeExists() {
    ValueProvider<String> location = StaticValueProvider.of(schemaFile.getPath());
    AvroSchemaStore store = AvroSchemaStore.of(location);
    assertTrue(store.docTypeExists("namespace_0", "foo"));
    assertTrue(store.docTypeExists("namespace_0", "bar"));
    assertTrue(store.docTypeExists("namespace_1", "baz"));
  }

  @Test
  public void testDocTypeExistsViaAttributes() {
    ValueProvider<String> location = StaticValueProvider.of(schemaFile.getPath());
    AvroSchemaStore store = AvroSchemaStore.of(location);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("document_namespace", "namespace_0");
    attributes.put("document_type", "foo");
    assertTrue(store.docTypeExists(attributes));
  }

}