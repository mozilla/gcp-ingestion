package com.mozilla.telemetry;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.io.Resources;
import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.rules.RedisServer;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DecoderMainTest extends TestWithDeterministicJson {

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  @Rule
  public final RedisServer redis = new RedisServer();

  @Test
  public void instantiateDecoderForCodeCoverage() {
    new Decoder();
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
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--errorOutputFileCompression=UNCOMPRESSED" });

    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(1));
  }

  @Test
  public void testMixedErrorCases() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String resourceDir = Resources.getResource("testdata/decoder-integration").getPath();
    String input = resourceDir + "/*-input.ndjson";
    String output = outputPath + "/out/out";
    String errorOutput = outputPath + "/error/error";

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--errorOutputType=file", "--errorOutput=" + errorOutput, "--includeStackTrace=false",
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputFileCompression=UNCOMPRESSED",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--redisUri=" + redis.uri });

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
    String resourceDir = Resources.getResource("testdata/decoder-integration").getPath();
    String input = resourceDir + "/gzipped.ndjson";
    String output = outputPath + "/out/out";
    String errorOutput = outputPath + "/error/error";

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=file",
        "--errorOutput=" + errorOutput, "--includeStackTrace=false",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--redisUri=" + redis.uri });

    List<String> outputLines = Lines.files(output + "*.ndjson");
    List<String> expectedOutputLines = Lines.files(resourceDir + "/output.ndjson");
    assertThat("Main output differed from expectation", outputLines,
        matchesInAnyOrder(expectedOutputLines));
  }

  /** Run the pipeline with the Pioneer decryption and decompression enabled. KMS is disabled since
   * it requires access to an external service. See the KeyStore integration tests for decrypting
   * private keys.
   */
  @Test
  public void testEncryptedPioneerPayload() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String resourceDir = Resources.getResource("testdata/decoder-integration").getPath();
    String input = resourceDir + "/pioneer.ndjson";
    String output = outputPath + "/out/out";
    String errorOutput = outputPath + "/error/error";
    String pioneerMetadataLocation = Resources.getResource("pioneer/metadata-decoder.json")
        .getPath();

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=file",
        "--errorOutput=" + errorOutput, "--includeStackTrace=false",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--redisUri=" + redis.uri, "--pioneerEnabled=true",
        "--pioneerMetadataLocation=" + pioneerMetadataLocation, "--pioneerKmsEnabled=false" });

    List<String> outputLines = Lines.files(output + "*.ndjson");
    List<String> expectedOutputLines = Lines.files(resourceDir + "/output.ndjson");
    assertThat("Main output differed from expectation", outputLines,
        matchesInAnyOrder(expectedOutputLines));
  }

  @Test
  public void testIdempotence() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String resourceDir = Resources.getResource("testdata/decoder-integration").getPath();
    String input = resourceDir + "/*-input.ndjson";
    String intermediateOutput = outputPath + "/out1/out1";
    String output = outputPath + "/out2/out2";
    String errorOutput = outputPath + "/error/error";

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", //
        "--input=" + input, //
        "--output=" + intermediateOutput, //
        "--errorOutput=" + errorOutput, //
        "--outputFileFormat=json", "--outputType=file", "--errorOutputType=file",
        "--includeStackTrace=false", "--outputFileCompression=UNCOMPRESSED",
        "--errorOutputFileCompression=UNCOMPRESSED",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--redisUri=" + redis.uri });

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", //
        "--input=" + intermediateOutput + "*.ndjson", //
        "--output=" + output, //
        "--errorOutput=" + errorOutput, //
        "--outputFileFormat=json", "--outputType=file", "--errorOutputType=file",
        "--includeStackTrace=false", "--outputFileCompression=UNCOMPRESSED",
        "--errorOutputFileCompression=UNCOMPRESSED",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--redisUri=" + redis.uri });

    List<String> outputLines = Lines.files(output + "*.ndjson");
    List<String> expectedOutputLines = Lines.files(resourceDir + "/output.ndjson");
    assertThat("Main output differed from expectation", outputLines,
        matchesInAnyOrder(expectedOutputLines));
  }
}
