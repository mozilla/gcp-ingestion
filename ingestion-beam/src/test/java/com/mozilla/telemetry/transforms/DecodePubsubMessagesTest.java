/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class DecodePubsubMessagesTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testText() {
    List<String> inputLines = Lines.resources("testdata/decode-pubsub-messages/input-*");

    WithErrors.Result<PCollection<PubsubMessage>> decoded = pipeline //
        .apply(Create.of(inputLines)) //
        .apply(InputFileFormat.text.decode());

    PCollection<String> encoded = decoded.output().apply(OutputFileFormat.text.encode());

    PAssert.that(encoded).containsInAnyOrder(inputLines);
    PAssert.that(decoded.errors()).empty();

    pipeline.run();
  }

  @Test
  public void testJson() {
    List<String> inputLines = Lines.resources("testdata/decode-pubsub-messages/input-*");
    List<String> validLines = Lines
        .resources("testdata/decode-pubsub-messages/output-normalized-json.ndjson");
    List<String> invalidLines = Lines
        .resources("testdata/decode-pubsub-messages/input-invalid-json.txt");

    WithErrors.Result<PCollection<PubsubMessage>> decoded = pipeline //
        .apply(Create.of(inputLines)) //
        .apply(InputFileFormat.json.decode());

    PCollection<String> encoded = decoded.output() //
        .apply("EncodeJsonOutput", OutputFileFormat.json.encode());

    PCollection<String> encodedErrors = decoded.errors() //
        .apply("EncodeTextError", OutputFileFormat.text.encode());

    PAssert.that(encoded).containsInAnyOrder(validLines);
    PAssert.that(encodedErrors).containsInAnyOrder(invalidLines);

    pipeline.run();
  }

}
