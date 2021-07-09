package com.mozilla.telemetry;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.mozilla.telemetry.decoder.rally.DecryptRallyPayloadsTest;
import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DecoderMainTest extends TestWithDeterministicJson {

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

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
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
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
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz" });

    List<String> outputLines = Lines.files(output + "*.ndjson");

    List<String> expectedOutputLines = Lines.files(resourceDir + "/output.ndjson");
    assertThat("Main output differed from expectation", outputLines,
        matchesInAnyOrder(expectedOutputLines));

    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");
    List<String> expectedErrorOutputLines = Lines.files(resourceDir + "/error-output.ndjson");
    assertThat("Error output differed from expectation", errorOutputLines,
        matchesInAnyOrder(expectedErrorOutputLines));
  }

  /** Helper for testEncryptedPioneerPayloads. The pipeline will insert metadata
   * during processing, which needs to be removed to match base payload. */
  private List<String> removeMetadata(List<String> lines) {
    return Arrays.asList(lines.stream().map(data -> {
      try {
        PubsubMessage message = Json.readPubsubMessage(data);
        String payload = DecryptRallyPayloadsTest
            .removeMetadata(new String(message.getPayload(), Charsets.UTF_8));
        // also remove any extra stuff from the telemetry namespace, since the test document
        // is a structured ingestion document
        ObjectNode node = Json.readObjectNode(payload);
        node.remove("normalized_app_name");
        node.remove("normalized_channel");

        return Json.asString(new PubsubMessage(Json.asBytes(node), message.getAttributeMap()));
      } catch (Exception e) {
        return null;
      }
    }).toArray(String[]::new));
  }

  private List<String> getPayload(List<String> lines) {
    return Arrays.asList(lines.stream().map(data -> {
      try {
        PubsubMessage message = Json.readPubsubMessage(data);
        return new String(message.getPayload(), Charsets.UTF_8);
      } catch (Exception e) {
        return null;
      }
    }).toArray(String[]::new));
  }

  private List<String> getErrorType(List<String> lines) {
    return Arrays.asList(lines.stream().map(data -> {
      try {
        PubsubMessage message = Json.readPubsubMessage(data);
        return message.getAttributeMap().get("error_type");
      } catch (Exception e) {
        return null;
      }
    }).toArray(String[]::new));
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
        "--errorOutputFileCompression=UNCOMPRESSED", "--errorOutput=" + errorOutput,
        "--includeStackTrace=false",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--pioneerEnabled=true",
        "--pioneerMetadataLocation=" + pioneerMetadataLocation, "--pioneerKmsEnabled=false" });

    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(0));

    List<String> outputLines = Lines.files(output + "*.ndjson");
    assertThat(outputLines, Matchers.hasSize(4));
    List<String> expectedOutputLines = Lines.files(resourceDir + "/output.ndjson");
    assertThat("Main output differed from expectation", getPayload(removeMetadata(outputLines)),
        matchesInAnyOrder(getPayload(expectedOutputLines)));
  }

  /** In this test, we only care that messages were correctly routed to the
   * Rally transform for decoding messages. The unit tests are more
   * comprehensive for the specific behavior of the transform. */
  @Test
  public void testEncryptedRallyPayload() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String resourceDir = Resources.getResource("testdata/decoder-integration").getPath();
    String input = resourceDir + "/rally.ndjson";
    String output = outputPath + "/out/out";
    String errorOutput = outputPath + "/error/error";
    String pioneerMetadataLocation = Resources.getResource("pioneer/metadata-decoder.json")
        .getPath();

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=file",
        "--errorOutputFileCompression=UNCOMPRESSED", "--errorOutput=" + errorOutput,
        "--includeStackTrace=false",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--pioneerEnabled=true",
        "--pioneerMetadataLocation=" + pioneerMetadataLocation, "--pioneerKmsEnabled=false" });

    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");
    assertThat(getErrorType(errorOutputLines), matchesInAnyOrder(
        Arrays.asList("DecryptRallyPayloads", "DecryptRallyPayloads", "DecryptRallyPayloads")));

  }

  /** Assert that the default value of --pioneerKms causes the unit test to fail
   * due to either a lack of GOOGLE_APPLICATION_CREDENTIALS or by an invalid
   * path to a KMS resource by the templating variables. Either of these cases
   * can be verified manually by removing the expected Exception value. If
   * GOOGLE_APPLICATION_CREDENTIALS is not set, then an IOException will be
   * propagated while trying to authenticate: "java.io.IOException: The
   * Application Default Credentials are not available." Otherwise a
   * RuntimeException will be encountered: "io.grpc.StatusRuntimeException:
   * NOT_FOUND: Project 'DUMMY_PROJECT_ID' not found."
  */
  @Test(expected = Exception.class)
  public void testEncryptedPioneerPayloadFailureByDefaultKmsEnabledInLocalTesting()
      throws Exception {
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
        "--schemasLocation=schemas.tar.gz", "--pioneerEnabled=true",
        "--pioneerMetadataLocation=" + pioneerMetadataLocation });
    // NOTE: equivalent to --pioneerKmsEnabled=true
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
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz" });

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", //
        "--input=" + intermediateOutput + "*.ndjson", //
        "--output=" + output, //
        "--errorOutput=" + errorOutput, //
        "--outputFileFormat=json", "--outputType=file", "--errorOutputType=file",
        "--includeStackTrace=false", "--outputFileCompression=UNCOMPRESSED",
        "--errorOutputFileCompression=UNCOMPRESSED",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz" });

    List<String> outputLines = Lines.files(output + "*.ndjson");
    List<String> expectedOutputLines = Lines.files(resourceDir + "/output.ndjson");
    assertThat("Main output differed from expectation", outputLines,
        matchesInAnyOrder(expectedOutputLines));
  }
}
