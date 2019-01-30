/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.io.Resources;
import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.rules.RedisServer;
import com.mozilla.telemetry.util.Json;
import java.lang.reflect.Field;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DecoderMainTest {

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  @Rule
  public final RedisServer redis = new RedisServer();

  @Test
  public void instantiateDecoderForCodeCoverage() {
    new Decoder();
  }

  /** Make serialization of attributes map deterministic for these tests. */
  @BeforeClass
  public static void setUp() throws Exception {
    Field mapperField = Json.class.getDeclaredField("MAPPER");
    mapperField.setAccessible(true);
    ObjectMapper mapper = (ObjectMapper) mapperField.get(null);
    mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  /** Reset the ObjectMapper configurations we changed. */
  @AfterClass
  public static void tearDown() throws Exception {
    Field mapperField = Json.class.getDeclaredField("MAPPER");
    mapperField.setAccessible(true);
    ObjectMapper mapper = (ObjectMapper) mapperField.get(null);
    mapper.disable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  @Test
  public void testBasicErrorOutput() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String input = Resources.getResource("testdata/single-message-input.json").getPath();
    String output = outputPath + "/out";
    String errorOutput = outputPath + "/error";

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--errorOutputType=file", "--errorOutput=" + errorOutput,
        "--geoCityDatabase=GeoLite2-City.mmdb", "--schemasLocation=schemas.tar.gz",
        "--seenMessagesSource=none", "--errorOutputFileCompression=UNCOMPRESSED" });

    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(1));
  }

  @Test
  public void testMixedErrorCases() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String resourceDir = "src/test/resources/testdata/decoder-integration";
    String input = resourceDir + "/*-input.ndjson";
    String output = outputPath + "/out/out";
    String errorOutput = outputPath + "/error/error";

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--errorOutputType=file", "--errorOutput=" + errorOutput, "--includeStackTrace=false",
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputFileCompression=UNCOMPRESSED",
        "--geoCityDatabase=GeoLite2-City.mmdb", "--schemasLocation=schemas.tar.gz",
        "--seenMessagesSource=none", "--redisUri=" + redis.uri });

    List<String> outputLines = Lines.files(output + "*.ndjson");
    List<String> expectedOutputLines = Lines.files(resourceDir + "/output.ndjson");
    assertThat("Main output differed from expectation", outputLines,
        matchesInAnyOrder(expectedOutputLines));

    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");
    List<String> expectedErrorOutputLines = Lines.files(resourceDir + "/error-output.ndjson");
    assertThat("Error output differed from expectation", errorOutputLines,
        matchesInAnyOrder(expectedErrorOutputLines));
  }

  @Test
  public void testGzippedPayload() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String resourceDir = "src/test/resources/testdata/decoder-integration";
    String input = resourceDir + "/gzipped.ndjson";
    String output = outputPath + "/out/out";
    String errorOutput = outputPath + "/error/error";

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=file",
        "--errorOutput=" + errorOutput, "--includeStackTrace=false",
        "--geoCityDatabase=GeoLite2-City.mmdb", "--schemasLocation=schemas.tar.gz",
        "--seenMessagesSource=none", "--redisUri=" + redis.uri });

    List<String> outputLines = Lines.files(output + "*.ndjson");
    List<String> expectedOutputLines = Lines.files(resourceDir + "/output.ndjson");
    assertThat("Main output differed from expectation", outputLines,
        matchesInAnyOrder(expectedOutputLines));
  }
}
