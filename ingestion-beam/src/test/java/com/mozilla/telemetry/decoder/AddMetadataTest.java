/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.ResultWithErrors;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;

public class AddMetadataTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    final List<String> input = Arrays.asList("{}", "{\"id\":null}", "[]", "{");
    ResultWithErrors<PCollection<PubsubMessage>> output = pipeline.apply(Create.of(input))
        .apply("decodeText", InputFileFormat.text.decode()).output()
        .apply("addAttributes", MapElements.into(new TypeDescriptor<PubsubMessage>() {
        }).via(element -> new PubsubMessage(element.getPayload(), ImmutableMap.of("meta", "data"))))
        .apply("addMetadata", new AddMetadata());

    final List<String> expectedMain = Arrays.asList("{\"metadata\":{\"meta\":\"data\"}}",
        "{\"metadata\":{\"meta\":\"data\"},\"id\":null}");
    final PCollection<String> main = output.output().apply("encodeTextMain",
        OutputFileFormat.text.encode());
    PAssert.that(main).containsInAnyOrder(expectedMain);

    final List<String> expectedError = Arrays.asList("{", "[]");
    final PCollection<String> error = output.errors().apply("encodeTextError",
        OutputFileFormat.text.encode());
    PAssert.that(error).containsInAnyOrder(expectedError);

    pipeline.run();
  }

}
