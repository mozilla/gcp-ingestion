/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.DecodePubsubMessages;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class SinkTest {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void decodeJsonAndEncodeJson() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":null,\"payload\":\"dGVzdA==\"}",
        "{\"payload\":\"dGVzdA==\"}",
        "{\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":null,\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":null,\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":null,\"payload\":\"\"}");

    final PCollection<String> output = pipeline
        .apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode())
        .get(DecodePubsubMessages.mainTag)
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void mainCanWriteToStdout() {
    String inputPath = this.getClass().getResource("single-message-input.json").getPath();

    // We are simply making sure this runs without throwing an exception.
    Sink.main(new String[]{
        "--inputFileFormat=json",
        "--inputType=file",
        "--input=" + inputPath,
        "--outputFileFormat=json",
        "--outputType=stdout"
    });
  }
}
