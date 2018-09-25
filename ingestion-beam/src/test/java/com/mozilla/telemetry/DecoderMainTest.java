/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static org.junit.Assert.assertThat;

import com.google.common.io.Resources;
import com.mozilla.telemetry.matchers.Lines;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DecoderMainTest {

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  private String outputPath;

  @Before
  public void initialize() {
    outputPath = outputFolder.getRoot().getAbsolutePath();
  }

  @Test
  public void testBasicErrorOutput() {
    String input = Resources.getResource("testdata/single-message-input.json").getPath();
    String output = outputPath + "/out";
    String errorOutput = outputPath + "/error";

    // We are simply making sure this runs without throwing an exception.
    Decoder.main(new String[]{
        "--inputFileFormat=json",
        "--inputType=file",
        "--input=" + input,
        "--outputFileFormat=json",
        "--outputType=file",
        "--output=" + output,
        "--errorOutputType=file",
        "--errorOutput=" + errorOutput,
        "--geoCityDatabase=GeoLite2-City.mmdb",
        "--seenMessagesSource=none"
    });

    List<String> errorOutputLines = Lines.files(errorOutput + "*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(1));
  }

}
