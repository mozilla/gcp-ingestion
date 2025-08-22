package com.mozilla.telemetry;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
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

  /**
   * Test ingestion of log entry payloads.
   * This tests two scenarios:
   * 1. A correct log entry payload that should be decoded successfully and
   *    have standard attributes applied (including Geo information).
   * 2. An incorrect log entry payload that is missing client_info that
   *    should be routed to the error output.
   */
  @Test
  public void testLogEntryPayload() {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String resourceDir = Resources.getResource("testdata/decoder-integration").getPath();
    String input = resourceDir + "/logentries.ndjson";
    String output = outputPath + "/out/out";
    String errorOutput = outputPath + "/error/error";

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--errorOutputType=file", "--errorOutput=" + errorOutput, "--includeStackTrace=false",
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputFileCompression=UNCOMPRESSED",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--logIngestionEnabled=true" });

    List<String> outputLines = Lines.files(output + "*.ndjson");
    List<String> expectedOutputLines = Lines.files(resourceDir + "/logentries-output.ndjson");
    assertThat("Main output differed from expectation", outputLines,
        matchesInAnyOrder(expectedOutputLines));

    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");
    List<String> expectedErrorLines = Lines.files(resourceDir + "/logentries-error-output.ndjson");
    assertThat("Error output differed from expectation", errorOutputLines,
        matchesInAnyOrder(expectedErrorLines));
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

  // Tests for republishing functionality, moved from RepublisherMainTest.

  @Test
  public void testDebugDestination() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String inputPath = Resources.getResource("testdata/republisher-integration").getPath();
    String input = inputPath + "/*.ndjson";
    String mainOutput = outputPath + "/out";
    String debugOutput = outputPath + "/debug_out";

    Decoder.main(new String[] {"--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + mainOutput,
        "--enableDebugDestination", "--debugDestination=" + debugOutput,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=stderr",
        // Decoder specific args
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz"});

    List<String> inputLines = Lines.files(inputPath + "/debug-messages.ndjson");
    List<String> outputLines = Lines.files(debugOutput + "/*.ndjson");
    assertThat(outputLines, matchesInAnyOrder(inputLines));
  }

  @Test
  public void testRandomSampleDestination() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String inputPath = Resources.getResource("testdata/republisher-integration").getPath();
    String input = inputPath + "/*.ndjson";
    String mainOutput = outputPath + "/out";
    String randomOutput = outputPath + "/random_out";

    Decoder.main(new String[] {"--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + mainOutput,
        "--randomSampleRatio=0.5", "--randomSampleDestination=" + randomOutput,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=stderr",
        // Decoder specific args
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz"});

    List<String> inputLines = Lines.files(inputPath + "/*.ndjson");
    List<String> outputLines = Lines.files(randomOutput + "/*.ndjson");
    assertThat(outputLines.size() * 1.0, Matchers.closeTo(inputLines.size() / 2.0, 10.0));
  }

  @Test
  public void testPerDocType() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String inputPath = Resources.getResource("testdata/republisher-integration").getPath();
    String input = inputPath + "/*.ndjson";
    String mainOutput = outputPath + "/out";
    String perDocTypePath = outputPath + "/per_doctype";
    String perDocTypeDestinations = "{\"" + perDocTypePath
        + "/out_telemetry_event\":[\"telemetry/event\"], \"" + perDocTypePath
        + "/out_bar_foo\": [\"bar/foo\"]}";

    Decoder.main(new String[] {"--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + mainOutput,
        "--perDocTypeDestinations=" + perDocTypeDestinations,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=stderr",
        // Decoder specific args
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz"});

    List<String> expectedLines = Lines.files(inputPath + "/per-doctype-*.ndjson");
    List<String> outputLines = Lines.files(perDocTypePath + "/*.ndjson");
    assertThat("Only specified docTypes should be published", outputLines,
        matchesInAnyOrder(expectedLines));

    List<String> expectedLinesEvent = Lines.files(inputPath + "/per-doctype-event.ndjson");
    List<String> outputLinesEvent = Lines.files(perDocTypePath + "/out_telemetry_event*.ndjson");
    assertThat("All docType=event messages are published", outputLinesEvent,
        matchesInAnyOrder(expectedLinesEvent));

    List<String> expectedLinesFoo = Lines.files(inputPath + "/per-doctype-foo.ndjson");
    List<String> outputLinesFoo = Lines.files(perDocTypePath + "/out_bar_foo*.ndjson");
    assertThat("All docType=foo messages are published", outputLinesFoo,
        matchesInAnyOrder(expectedLinesFoo));
  }

  @Test
  public void testPerNamespace() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String inputPath = Resources.getResource("testdata/republisher-integration").getPath();
    String input = inputPath + "/*.ndjson";
    String mainOutput = outputPath + "/out";
    String perNamespacePath = outputPath + "/per_namespace";
    String perNamespaceDestinations =
        "{\"mynamespace\":\"" + perNamespacePath + "/per-namespace_mynamespace" + "\"}";

    Decoder.main(new String[] {"--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + mainOutput,
        "--perNamespaceDestinations=" + perNamespaceDestinations,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=stderr",
        // Decoder specific args
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz"});

    List<String> expectedLines = Lines.files(inputPath + "/per-namespace-*.ndjson");
    List<String> outputLines = Lines.files(perNamespacePath + "/*.ndjson");
    assertThat("Only specified namespaces should be published", outputLines,
        matchesInAnyOrder(expectedLines));

    List<String> expectedLinesMyNamespace =
        Lines.files(inputPath + "/per-namespace-mynamespace.ndjson");
    List<String> outputLinesMyNamespace =
        Lines.files(perNamespacePath + "/per-namespace_mynamespace*.ndjson");
    assertThat("All namespace=mynamespace messages are published", outputLinesMyNamespace,
        matchesInAnyOrder(expectedLinesMyNamespace));
  }

  @Test
  public void testPerChannel() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String inputPath = Resources.getResource("testdata/republisher-integration").getPath();
    String input = inputPath + "/per-channel-*.ndjson";
    String mainOutput = outputPath + "/out";
    String perChannelPath = outputPath + "/per_channel";
    String perChannelDestination = perChannelPath + "/out-${channel}";

    Decoder.main(new String[] {"--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + mainOutput,
        "--perChannelDestination=" + perChannelDestination,
        "--outputFileCompression=UNCOMPRESSED",
        "--perChannelSampleRatios={\"beta\":1.0,\"release\":0.5}", "--errorOutputType=stderr",
        // Decoder specific args
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz"});

    List<String> inputLinesBeta = Lines.files(inputPath + "/per-channel-beta.ndjson");
    List<String> outputLinesBeta = Lines.files(perChannelPath + "/out-beta*.ndjson");
    assertThat(outputLinesBeta, matchesInAnyOrder(inputLinesBeta));

    List<String> outputLinesNightly = Lines.files(perChannelPath + "/out-nightly*.ndjson");
    assertThat(outputLinesNightly, Matchers.hasSize(0));

    List<String> outputLinesRelease = Lines.files(perChannelPath + "/out-release*.ndjson");
    assertThat("50% random sample of 20 elements should produce at least 2",
        outputLinesRelease.size(), Matchers.greaterThanOrEqualTo(2));
    assertThat("50% random sample of 20 elements should produce at most 18",
        outputLinesRelease.size(), Matchers.lessThanOrEqualTo(18));
  }
}
