package com.mozilla.telemetry;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.io.Resources;
import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Collections;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class SinkMainTest extends TestWithDeterministicJson {

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private String outputPath;

  @Before
  public void initialize() {
    outputPath = outputFolder.getRoot().getAbsolutePath();
  }

  @Test
  public void instantiateSinkForCodeCoverage() {
    new Sink();
  }

  @Test
  public void canWriteToStdout() {
    String input = Resources.getResource("testdata/single-message-input.json").getPath();

    // We are simply making sure this runs without throwing an exception.
    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=stdout", "--errorOutputType=stderr" });
  }

  @Test
  public void canWriteToStderr() {
    String input = Resources.getResource("testdata/single-message-input.json").getPath();

    // We are simply making sure this runs without throwing an exception.
    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=stderr", "--errorOutputType=stderr" });
  }

  @Test
  public void throwsExceptionOnDefaultlessPlaceholder() {
    String input = Resources.getResource("testdata/single-message-input.json").getPath();
    thrown.expectMessage(Matchers.containsString("defaultless placeholder"));
    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file",
        "--output=tmp/${some_nonexistent_attribute}/out", "--errorOutputType=stderr" });
  }

  @Test
  public void testAttributePlaceholders() {
    String inputPath = Resources.getResource("testdata/attribute-placeholders").getPath();
    String input = inputPath + "/*.ndjson";
    String output = outputPath + "/${host:-no_host}/${geo_city:-no_city}/out";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=stderr" });

    assertThat(Lines.files(outputPath + "/**/out*.ndjson"),
        matchesInAnyOrder(Lines.files(inputPath + "/*.ndjson")));

    assertThat(Lines.files(outputPath + "/hostA/cityA/out*.ndjson"),
        matchesInAnyOrder(Lines.files(inputPath + "/hostA-cityA.ndjson")));

    assertThat(Lines.files(outputPath + "/hostA/cityB/out*.ndjson"),
        matchesInAnyOrder(Lines.files(inputPath + "/hostA-cityB.ndjson")));

    assertThat(Lines.files(outputPath + "/no_host/cityA/out*.ndjson"),
        matchesInAnyOrder(Lines.files(inputPath + "/null-cityA.ndjson")));

    assertThat(Lines.files(outputPath + "/no_host/cityB/out*.ndjson"),
        matchesInAnyOrder(Collections.emptyList()));
  }

  @Test
  public void canWriteSingleRecord() {
    String input = Resources.getResource("testdata/single-message-input.json").getPath();
    String output = outputPath + "/out";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=stderr" });

    List<String> inputLines = Lines.files(input);
    List<String> outputLines = Lines.files(outputPath + "/out*.ndjson");

    assertThat(outputLines, matchesInAnyOrder(inputLines));
  }

  @Test
  public void testTextToJson() {
    String input = Resources.getResource("testdata/basic-messages-payloads.txt").getPath();
    String output = outputPath + "/out";

    Sink.main(new String[] { "--inputFileFormat=text", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=stderr" });

    List<String> outputLines = Lines.files(outputPath + "/out*.ndjson");
    List<String> expected = Lines.resources("testdata/basic-messages-valid-null-attributes.ndjson");

    assertThat(outputLines, matchesInAnyOrder(expected));
  }

  @Test
  public void testJsonToText() {
    String input = Resources.getResource("testdata/basic-messages-valid.ndjson").getPath();
    String output = outputPath + "/out";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=text", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=stderr" });

    List<String> outputLines = Lines.files(outputPath + "/out*.txt");
    List<String> expected = Lines.resources("testdata/basic-messages-payloads.txt");

    assertThat(outputLines, matchesInAnyOrder(expected));
  }

  @Test
  public void testParseTimestamp() throws Exception {
    String inputPath = Resources.getResource("testdata").getPath();
    String input = inputPath + "/basic-messages-valid-*.ndjson";
    String output = outputPath + "/${submission_date:-NULL}/${submission_hour:-NULL}/out";

    Sink.main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
        "--outputFileFormat=json", "--outputType=file", "--output=" + output,
        "--outputFileCompression=UNCOMPRESSED", "--errorOutputType=stderr" });

    List<String> nullLines = Lines
        .resources("testdata/basic-messages-valid-null-attributes.ndjson");
    List<String> timestampLines = Lines
        .resources("testdata/basic-messages-valid-with-timestamp.ndjson");

    List<String> outputNullLines = Lines.files(outputPath + "/NULL/NULL/out*.ndjson");
    List<String> outputTimestampLines = Lines.files(outputPath + "/2018-03-12/21/out*.ndjson");

    assertThat(outputNullLines, matchesInAnyOrder(nullLines));
    assertThat(outputTimestampLines, Matchers.hasSize(timestampLines.size()));
  }

}
