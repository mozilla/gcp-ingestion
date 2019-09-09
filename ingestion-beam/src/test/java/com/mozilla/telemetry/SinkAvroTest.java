/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import com.mozilla.telemetry.schemas.AvroSchemaStore;
import com.mozilla.telemetry.schemas.SchemaNotFoundException;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/** 
   The Avro tests are more involved than the file-based tests in SinkMainTest
   because there are more preconditions necessary for testing. Each of the
   documents require the following metadata to be attached to the payload:

   - document_namespace
   - document_type
   - document_version

   The documents will need to have a corresponding store. The schemas are
   documented in `bin/generate-avro-test-resources`.
 */
public class SinkAvroTest {

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private String outputPath;

  @Before
  public void initialize() {
    outputPath = outputFolder.getRoot().getAbsolutePath();
  }

  private long getPrefixFileCount(String path, String prefix) {
    try {
      return Files.walk(Paths.get(path)).filter(Files::isRegularFile)
          .filter(p -> p.toFile().getName().startsWith(prefix)).count();
    } catch (IOException e) {
      return -1;
    }
  }

  @Test(expected = RuntimeException.class)
  public void testSinkThrowsOnInvalidOutput() {
    String input = Resources.getResource("testdata/avro-message-single-doctype.ndjson").getPath();
    String schemas = Resources.getResource("avro/test-schema.tar.gz").getPath();
    String output = outputPath + "/out";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputType=avro", "--output=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--schemasLocation=" + schemas, "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutputType=stdout" });
  }

  /**
   * Test for a single doctype being written out to the correct location.
   */
  @Test
  public void testSingleDocumentType() throws IOException, SchemaNotFoundException {
    String input = Resources.getResource("testdata/avro-message-single-doctype.ndjson").getPath();
    String schemas = Resources.getResource("avro/test-schema.tar.gz").getPath();
    String output = outputPath
        + "/${document_namespace:-NONE}.${document_type:-NONE}.${document_version:-0}";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputType=avro", "--output=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--schemasLocation=" + schemas, "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutputType=stdout" });

    assertThat("output count", getPrefixFileCount(outputPath, "namespace_0"),
        Matchers.greaterThan(0L));

    AvroSchemaStore store = AvroSchemaStore.of(StaticValueProvider.of(schemas),
        StaticValueProvider.of(null));

    List<Path> paths = Files.walk(Paths.get(outputPath)).filter(Files::isRegularFile)
        .collect(Collectors.toList());

    List<Integer> results = new ArrayList<>();
    for (Path path : paths) {
      Schema schema = store.getSchema("namespace_0/foo/foo.1.avro.json");
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      DataFileReader<GenericRecord> fileReader = new DataFileReader<>(path.toFile(), reader);
      while (fileReader.hasNext()) {
        GenericRecord record = (GenericRecord) fileReader.next();
        results.add((Integer) record.get("test_int"));
      }
      fileReader.close();
    }
    results.sort(null);
    assertEquals(results, Arrays.asList(1, 2, 3));
  }

  private Path getPath(String outputPath, String prefix) throws IOException {
    return Files.walk(Paths.get(outputPath)).filter(Files::isRegularFile)
        .filter(p -> p.toFile().getName().startsWith(prefix)).findFirst().get();
  }

  private GenericRecord readRecord(AvroSchemaStore store, String outputPath, String schemaPath)
      throws IOException, SchemaNotFoundException {
    Path path = getPath(outputPath, schemaPath);
    String[] p = schemaPath.split("\\.");
    String schemaStorePath = String.format("%s/%s/%s.%s.avro.json", p[0], p[1], p[1], p[2]);
    Schema schema = store.getSchema(schemaStorePath);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
    try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(path.toFile(),
        datumReader)) {
      return (GenericRecord) fileReader.next();
    }
  }

  /**
   * Test that documents with existing schemas are being written out to the correct location.
   */
  @Test
  public void testMultipleDocumentTypes() throws IOException, SchemaNotFoundException {
    String input = Resources.getResource("testdata/avro-message-multiple-doctype.ndjson").getPath();
    String schemas = Resources.getResource("avro/test-schema.tar.gz").getPath();
    String output = outputPath
        + "/${document_namespace:-NONE}.${document_type:-NONE}.${document_version:-0}";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputType=avro", "--output=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--schemasLocation=" + schemas, "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutputType=stdout" });

    assertThat("foo output count", getPrefixFileCount(outputPath, "namespace_0.foo"),
        Matchers.greaterThan(0L));
    assertThat("bar output count", getPrefixFileCount(outputPath, "namespace_0.bar"),
        Matchers.greaterThan(0L));
    assertThat("baz output count", getPrefixFileCount(outputPath, "namespace_1.baz"),
        Matchers.greaterThan(0L));

    AvroSchemaStore store = AvroSchemaStore.of(StaticValueProvider.of(schemas),
        StaticValueProvider.of(null));
    assertEquals(1, readRecord(store, outputPath, "namespace_0.foo.1").get("test_int"));
    assertEquals(1, readRecord(store, outputPath, "namespace_0.bar.1").get("test_int"));
    assertEquals(null, readRecord(store, outputPath, "namespace_1.baz.1").get("test_null"));
  }

  /**
   * Test that for invalid documents. This case includes a ping where the data
   * wouldn't have passed validation and a document where a schema doesn't
   * exist. These documents should go to the error collection so they can be
   * reprocessed.
   */
  @Test
  public void testInvalidDocuments() throws IOException {
    String input = Resources.getResource("testdata/avro-message-invalid-doctype.ndjson").getPath();
    String schemas = Resources.getResource("avro/test-schema.tar.gz").getPath();
    String output = outputPath
        + "/${document_namespace:-NONE}.${document_type:-NONE}.${document_version:-0}";
    String errorOutput = outputPath + "/err/${document_namespace:-NONE}";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputType=avro", "--output=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--schemasLocation=" + schemas, "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutputType=file", "--errorOutput=" + errorOutput, });

    assertThat("error ns_0 count", getPrefixFileCount(outputPath + "/err", "namespace_0"),
        Matchers.greaterThan(0L));
    assertThat("error ns_57 count", getPrefixFileCount(outputPath + "/err", "namespace_57"),
        Matchers.greaterThan(0L));

    String data = null;
    ObjectNode obj = null;
    String msg = null;

    // Case where the schema is an integer but the value is a string
    data = new String(Files.readAllBytes(getPath(outputPath + "/err", "namespace_0")));
    obj = Json.readObjectNode(data.getBytes(StandardCharsets.UTF_8));
    msg = obj.path("attributeMap").path("error_message").textValue();
    assertThat(msg, CoreMatchers.containsString("org.apache.avro.AvroTypeException"));

    // Case where the schema does not exist in the schema store
    data = new String(Files.readAllBytes(getPath(outputPath + "/err", "namespace_57")));
    obj = Json.readObjectNode(data.getBytes(StandardCharsets.UTF_8));
    msg = obj.path("attributeMap").path("error_message").textValue();
    assertThat(msg,
        CoreMatchers.containsString("com.mozilla.telemetry.schemas.SchemaNotFoundException"));
  }
}
