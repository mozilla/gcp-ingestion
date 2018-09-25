/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.io.Resources;
import com.mozilla.telemetry.matchers.Lines;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SinkMainTest {

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  private String outputPath;

  @Before
  public void initialize() {
    outputPath = outputFolder.getRoot().getAbsolutePath();
  }

  @Test
  public void canWriteToStdout() {
    String input = Resources.getResource("testdata/single-message-input.json").getPath();

    // We are simply making sure this runs without throwing an exception.
    Sink.main(new String[]{
        "--inputFileFormat=json",
        "--inputType=file",
        "--input=" + input,
        "--outputFileFormat=json",
        "--outputType=stdout"
    });
  }

  @Test(expected = PipelineExecutionException.class)
  public void throwsExceptionOnNullPlaceholder() {
    String input = Resources.getResource("testdata/single-message-input.json").getPath();

    Sink.main(new String[]{
        "--inputFileFormat=json",
        "--inputType=file",
        "--input=" + input,
        "--outputFileFormat=json",
        "--outputType=file",
        "--output=tmp/${some_nonexistent_attribute}/out"
    });
  }

  @Test
  public void testAttributePlaceholders() {
    String inputPath = Resources.getResource("testdata/attribute-placeholders").getPath();
    String input = inputPath + "/*.ndjson";
    String output = outputPath + "/${host:-no_host}/${geo_city}/out";

    Sink.main(new String[]{
        "--inputFileFormat=json",
        "--inputType=file",
        "--input=" + input,
        "--outputFileFormat=json",
        "--outputType=file",
        "--output=" + output
    });

    assertThat(Lines.files(outputPath + "/**/out*.ndjson"), matchesInAnyOrder(
               Lines.files(inputPath + "/*.ndjson")));

    assertThat(Lines.files(outputPath + "/hostA/cityA/out*.ndjson"), matchesInAnyOrder(
               Lines.files(inputPath + "/hostA-cityA.ndjson")));

    assertThat(Lines.files(outputPath + "/hostA/cityB/out*.ndjson"), matchesInAnyOrder(
               Lines.files(inputPath + "/hostA-cityB.ndjson")));

    assertThat(Lines.files(outputPath + "/no_host/cityA/out*.ndjson"), matchesInAnyOrder(
               Lines.files(inputPath + "/null-cityA.ndjson")));

    assertThat(Lines.files(outputPath + "/no_host/cityB/out*.ndjson"), matchesInAnyOrder(
        Collections.emptyList()));
  }

  @Test
  public void canWriteSingleRecord() {
    String input = Resources.getResource("testdata/single-message-input.json").getPath();
    String output = outputPath + "/out";

    Sink.main(new String[]{
        "--inputFileFormat=json",
        "--inputType=file",
        "--input=" + input,
        "--outputFileFormat=json",
        "--outputType=file",
        "--output=" + output
    });

    List<String> inputLines = Lines.files(input);
    List<String> outputLines = Lines.files(outputPath + "/out*.ndjson");

    assertThat(outputLines, matchesInAnyOrder(inputLines));
  }

  @Test
  public void canWriteErrorOutput() {
    String input = Resources.getResource("testdata/basic-messages-mixed-validity.ndjson").getPath();
    String output = outputPath + "/valid/out";
    String errorOutput = outputPath + "/error/out";

    Sink.main(new String[]{
        "--inputFileFormat=json",
        "--inputType=file",
        "--input=" + input,
        "--outputFileFormat=json",
        "--outputType=file",
        "--output=" + output,
        "--errorOutputType=file",
        "--errorOutput=" + errorOutput
    });

    List<String> validLines = Lines.resources("testdata/basic-messages-valid.ndjson");
    List<String> invalidLines = Lines.resources("testdata/basic-messages-invalid.ndjson");

    List<String> outputLines = Lines.files(outputPath + "/valid/out*.ndjson");
    List<String> errorOutputLines = Lines.files(outputPath + "/error/out*.ndjson");

    assertThat(outputLines, matchesInAnyOrder(validLines));
    assertThat(errorOutputLines, Matchers.hasSize(invalidLines.size()));
  }

  @Test
  public void testTextToJson() {
    String input = Resources.getResource("testdata/basic-messages-payloads.txt").getPath();
    String output = outputPath + "/out";

    Sink.main(new String[]{
        "--inputFileFormat=text",
        "--inputType=file",
        "--input=" + input,
        "--outputFileFormat=json",
        "--outputType=file",
        "--output=" + output
    });

    List<String> outputLines = Lines.files(outputPath + "/out*.ndjson");
    List<String> expected = Lines.resources("testdata/basic-messages-valid-null-attributes.ndjson");

    assertThat(outputLines, matchesInAnyOrder(expected));
  }

  @Test
  public void testJsonToText() {
    String input = Resources.getResource("testdata/basic-messages-valid.ndjson").getPath();
    String output = outputPath + "/out";

    Sink.main(new String[]{
        "--inputFileFormat=json",
        "--inputType=file",
        "--input=" + input,
        "--outputFileFormat=text",
        "--outputType=file",
        "--output=" + output
    });

    List<String> outputLines = Lines.files(outputPath + "/out*.txt");
    List<String> expected = Lines.resources("testdata/basic-messages-payloads.txt");

    assertThat(outputLines, matchesInAnyOrder(expected));
  }

}
