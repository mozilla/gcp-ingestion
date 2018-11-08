/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

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

public class ParseUriTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":" + "{\"uri\":\"/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648"
            + "/main/Firefox/61.0a1/nightly/20180328030202\"" + "},\"payload\":\"\"}",
        "{\"attributeMap\":"
            + "{\"uri\":\"/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049\""
            + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":" + "{\"app_name\":\"Firefox\"" + ",\"app_version\":\"61.0a1\""
            + ",\"app_build_id\":\"20180328030202\"" + ",\"app_update_channel\":\"nightly\""
            + ",\"document_namespace\":\"telemetry\""
            + ",\"document_id\":\"ce39b608-f595-4c69-b6a6-f7a436604648\""
            + ",\"document_type\":\"main\"" + "},\"payload\":\"\"}",
        "{\"attributeMap\":" + "{\"document_namespace\":\"eng-workflow\""
            + ",\"document_version\":\"1\""
            + ",\"document_id\":\"2c3a0767-d84a-4d02-8a92-fa54a3376049\""
            + ",\"document_type\":\"hgpush\"" + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline.apply(Create.of(input))
        .apply("decodeJson", InputFileFormat.json.decode()).get(DecodePubsubMessages.mainTag)
        .apply("parseUri", new ParseUri()).get(ParseUri.mainTag)
        .apply("encodeJson", OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

}
