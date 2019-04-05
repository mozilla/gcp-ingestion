/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static org.junit.Assert.assertThat;

import com.google.common.io.Resources;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
   documented in `bin/generate-avro-test-resources.py`.
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

  /**
   * Test for a single doctype being written out to the correct location.
   */
  @Test
  public void testSingleDocumentType() {
    String input = Resources.getResource("testdata/avro-message-single-doctype.ndjson").getPath();
    String schemas = Resources.getResource("testdata/avro-schema-test.tar.gz").getPath();
    String output = outputPath + "/out";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputType=avro", "--output=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--schemaLocation=" + schemas, "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutputType=stdout" });

    assertThat("output count", getPrefixFileCount(outputPath, "out"), Matchers.greaterThan(0L));
  }

  /**
   * Test that documents with existing schemas are being written out to the correct location.
   */
  @Test
  public void testMultipleDocumentTypes() {
    String input = Resources.getResource("testdata/avro-message-multiple-doctype.ndjson").getPath();
    String schemas = Resources.getResource("testdata/avro-schema-test.tar.gz").getPath();
    String output = outputPath + "/${document_type:-default}/out";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputType=avro", "--output=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--schemaLocation=" + schemas, "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutputType=stdout" });

    assertThat("foo output count", getPrefixFileCount(outputPath + "/foo", "out"),
        Matchers.greaterThan(0L));
    assertThat("bar output count", getPrefixFileCount(outputPath + "/bar", "out"),
        Matchers.greaterThan(0L));
    assertThat("baz output count", getPrefixFileCount(outputPath + "/baz", "out"),
        Matchers.greaterThan(0L));

  }

  /**
   * Test that for invalid documents. This case includes a ping where the data
   * wouldn't have passed validation and a document where a schema doesn't
   * exist. These documents should go to the error collection so they can be
   * reprocessed.
   */
  @Test
  public void testInvalidDocuments() {
    String input = Resources.getResource("testdata/avro-message-invalid-doctype.ndjson").getPath();
    String schemas = Resources.getResource("testdata/avro-schema-test.tar.gz").getPath();
    String output = outputPath + "/${document_type:-default}/out";
    String errorOutput = outputPath + "/err";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputType=avro", "--output=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--schemaLocation=" + schemas, "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutputType=file", "--errorOutput=" + errorOutput, });

    assertThat("error count", getPrefixFileCount(outputPath, "err"), Matchers.greaterThan(0L));
    assertThat("foo output count", getPrefixFileCount(outputPath + "/foo", "out"),
        Matchers.is(-1L));
  }
}
