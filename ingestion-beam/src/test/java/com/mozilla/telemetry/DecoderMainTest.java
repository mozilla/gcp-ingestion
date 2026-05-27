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
import java.util.Map;
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

  /**
   * Test ingestion of direct-Pub/Sub payloads.
   *
   * <p>The input file holds two messages in the attributes-shape: one with all routing
   * attributes + user_agent + x_forwarded_for and an uncompressed body, and one with only
   * required attributes and a gzipped body. We confirm that
   * <ul>
   *   <li>document_* attributes flow through to output unchanged,</li>
   *   <li>{@code submission_timestamp} is stamped by the pipeline (any value),</li>
   *   <li>the gzipped body is decompressed (asserted via {@code client_compression}),</li>
   *   <li>x_forwarded_for drives geo lookup on the main stage.</li>
   * </ul>
   * Exact-value behavior of {@code StampSubmissionTimestamp} is covered by its unit test.
   */
  @Test
  public void testDirectPubsubPayload() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String resourceDir = Resources.getResource("testdata/decoder-integration").getPath();
    String input = resourceDir + "/directpubsub.ndjson";
    String output = outputPath + "/out/out";
    String errorOutput = outputPath + "/error/error";

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--errorOutputType=file", "--errorOutput=" + errorOutput, "--includeStackTrace=false",
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputFileCompression=UNCOMPRESSED",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--directPubsubEnabled=true" });

    List<String> outputLines = Lines.files(output + "*.ndjson");
    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");

    assertThat("All direct-pubsub messages should land in main output, none in error",
        errorOutputLines, Matchers.empty());
    assertThat("Expected one output message per input", outputLines, Matchers.hasSize(2));

    PubsubMessage uncompressed = null;
    PubsubMessage gzipped = null;
    for (String line : outputLines) {
      PubsubMessage message = Json.readPubsubMessage(line);
      String documentId = message.getAttribute("document_id");
      if ("2c3a0767-d84a-4d02-8a92-fa54a3376048".equals(documentId)) {
        uncompressed = message;
      } else if ("3c3a0767-d84a-4d02-8a92-fa54a3376051".equals(documentId)) {
        gzipped = message;
      }
    }
    assertThat("Uncompressed message present", uncompressed, Matchers.notNullValue());
    assertThat("Gzipped message present", gzipped, Matchers.notNullValue());

    Map<String, String> a1 = uncompressed.getAttributeMap();
    assertThat(a1.get("document_namespace"), Matchers.equalTo("test"));
    assertThat(a1.get("document_type"), Matchers.equalTo("test"));
    assertThat(a1.get("document_version"), Matchers.equalTo("1"));
    assertThat("submission_timestamp must be stamped", a1.get("submission_timestamp"),
        Matchers.notNullValue());
    assertThat("Geo lookup on x_forwarded_for from attribute", a1.get("geo_country"),
        Matchers.equalTo("PH"));
    assertThat("user_agent attribute passes through, parsed by ParseUserAgent",
        a1.get("user_agent_browser"), Matchers.equalTo("Firefox"));
    assertThat("x_forwarded_for must be scrubbed before output", a1.get("x_forwarded_for"),
        Matchers.nullValue());

    Map<String, String> a2 = gzipped.getAttributeMap();
    assertThat(a2.get("document_namespace"), Matchers.equalTo("test"));
    assertThat(a2.get("document_type"), Matchers.equalTo("test"));
    assertThat(a2.get("document_version"), Matchers.equalTo("1"));
    assertThat("submission_timestamp must be stamped", a2.get("submission_timestamp"),
        Matchers.notNullValue());
    assertThat("Gzipped body must be decompressed and recorded", a2.get("client_compression"),
        Matchers.equalTo("gzip"));
  }

  /**
   * Direct-Pub/Sub messages missing any required routing attribute must be rejected to error
   * output rather than passed through. This complements the unit test in
   * {@code StampSubmissionTimestampTest} by exercising the full pipeline wiring.
   */
  @Test
  public void testDirectPubsubMissingRequiredAttributeGoesToError() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String resourceDir = Resources.getResource("testdata/decoder-integration").getPath();
    String input = resourceDir + "/directpubsub-invalid.ndjson";
    String output = outputPath + "/out/out";
    String errorOutput = outputPath + "/error/error";

    Decoder.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--errorOutputType=file", "--errorOutput=" + errorOutput, "--includeStackTrace=false",
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputFileCompression=UNCOMPRESSED",
        "--geoCityDatabase=src/test/resources/cityDB/GeoIP2-City-Test.mmdb",
        "--geoIspDatabase=src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb",
        "--schemasLocation=schemas.tar.gz", "--directPubsubEnabled=true" });

    List<String> outputLines = Lines.files(output + "*.ndjson");
    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");

    assertThat("No invalid messages should reach main output", outputLines, Matchers.empty());
    assertThat("Each invalid message must be routed to error output", errorOutputLines,
        Matchers.hasSize(4));
    for (String line : errorOutputLines) {
      PubsubMessage errorMessage = Json.readPubsubMessage(line);
      assertThat("Failure must be attributed to StampSubmissionTimestamp",
          errorMessage.getAttribute("error_type"), Matchers.equalTo("StampSubmissionTimestamp"));
    }
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
