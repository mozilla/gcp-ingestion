/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

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

public class RepublisherMainTest extends TestWithDeterministicJson {

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  @Rule
  public final RedisServer redis = new RedisServer();

  @Test
  public void instantiateRepublisherForCodeCoverage() {
    new Republisher();
  }

  @Test
  public void testDebugDestination() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String inputPath = Resources.getResource("testdata/republisher-integration").getPath();
    String input = inputPath + "/*.ndjson";
    String output = outputPath + "/out";

    Republisher
        .main(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
            "--outputFileFormat=json", "--outputType=file", "--enableDebugDestination",
            "--debugDestination=" + output, "--outputFileCompression=UNCOMPRESSED" });

    List<String> inputLines = Lines.files(inputPath + "/debug-messages.ndjson");
    List<String> outputLines = Lines.files(outputPath + "/out*.ndjson");
    assertThat(outputLines, matchesInAnyOrder(inputLines));
  }

  @Test
  public void testPerDocType() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String inputPath = Resources.getResource("testdata/republisher-integration").getPath();
    String input = inputPath + "/*.ndjson";
    String output = outputPath + "/out-${document_namespace}-${document_type}";

    Republisher.main(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputFileFormat=json", "--outputType=file",
        "--perDocTypeDestination=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--perDocTypeEnabledList=event,bar/foo,mynamespace/*", "--redisUri=" + redis.uri });

    List<String> inputLines = Lines.files(inputPath + "/per-doctype-*.ndjson");
    List<String> outputLines = Lines.files(outputPath + "/out*.ndjson");
    assertThat("Only specified docTypes should be published", outputLines,
        matchesInAnyOrder(inputLines));

    List<String> inputLinesEvent = Lines.files(inputPath + "/per-doctype-event.ndjson");
    List<String> outputLinesEvent = Lines.files(outputPath + "/out-telemetry-event*.ndjson");
    assertThat("All docType=event messages are published", outputLinesEvent,
        matchesInAnyOrder(inputLinesEvent));

    List<String> inputLinesFoo = Lines.files(inputPath + "/per-doctype-foo.ndjson");
    List<String> outputLinesFoo = Lines.files(outputPath + "/out-bar-foo*.ndjson");
    assertThat("All docType=foo messages are published", outputLinesFoo,
        matchesInAnyOrder(inputLinesFoo));

    List<String> inputLinesNamespace = Lines.files(inputPath + "/per-doctype-mynamespace.ndjson");
    List<String> outputLinesNamespace = Lines.files(outputPath + "/out-mynamespace-all*.ndjson");
    assertThat("All namespace=mynamespace messages are published", outputLinesNamespace,
        matchesInAnyOrder(inputLinesNamespace));
  }

  @Test
  public void testPerChannel() throws Exception {
    String outputPath = outputFolder.getRoot().getAbsolutePath();
    String inputPath = Resources.getResource("testdata/republisher-integration").getPath();
    String input = inputPath + "/per-channel-*.ndjson";
    String output = outputPath + "/out-${channel}";

    Republisher.main(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputFileFormat=json", "--outputType=file",
        "--perChannelDestination=" + output, "--outputFileCompression=UNCOMPRESSED",
        "--perChannelSampleRatios={\"beta\":1.0,\"release\":0.5}", "--redisUri=" + redis.uri });

    List<String> inputLinesBeta = Lines.files(inputPath + "/per-channel-beta.ndjson");
    List<String> outputLinesBeta = Lines.files(outputPath + "/out-beta*.ndjson");
    assertThat(outputLinesBeta, matchesInAnyOrder(inputLinesBeta));

    List<String> outputLinesNightly = Lines.files(outputPath + "/out-nightly*.ndjson");
    assertThat(outputLinesNightly, Matchers.hasSize(0));

    List<String> outputLinesRelease = Lines.files(outputPath + "/out-release*.ndjson");
    assertThat("50% random sample of 20 elements should produce at least 2",
        outputLinesRelease.size(), Matchers.greaterThanOrEqualTo(2));
    assertThat("50% random sample of 20 elements should produce at most 18",
        outputLinesRelease.size(), Matchers.lessThanOrEqualTo(18));
  }

}
