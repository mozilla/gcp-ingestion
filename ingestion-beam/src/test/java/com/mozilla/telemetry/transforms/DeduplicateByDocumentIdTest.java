/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class DeduplicateByDocumentIdTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    final List<String> input = Arrays.asList(
        // Non-duplicated record
        "{\"attributeMap\":{\"document_id\":\"not duplicated\""
            + ",\"submission_timestamp\":\"2020-01-12T21:03:18.234567Z\"}"
            + ",\"payload\":\"dGVzdA==\"}",
        // No document_id
        "{\"attributeMap\":{}" + ",\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{}" + ",\"payload\":\"dGVzdA==\"}",
        // Missing and invalid timestamps
        "{\"attributeMap\":{\"document_id\":\"strange timestamps\""
            + ",\"submission_timestamp\":\"2020-01-12F\"}" + ",\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"document_id\":\"strange timestamps\""
            + ",\"submission_timestamp\":\"2020-01-12T21:03:18.234567Z\"}"
            + ",\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"document_id\":\"strange timestamps\"}" + ",\"payload\":\"dGVzdA==\"}",
        // Record that appears three times with different timestamps.
        "{\"attributeMap\":{\"document_id\":\"foo\""
            + ",\"submission_timestamp\":\"2020-01-12T21:03:18.234567Z\"}"
            + ",\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"document_id\":\"foo\""
            + ",\"submission_timestamp\":\"2020-01-12T21:03:18.123456Z\"}"
            + ",\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"document_id\":\"foo\""
            + ",\"submission_timestamp\":\"2020-01-12T21:02:18.123456Z\"}"
            + ",\"payload\":\"dGVzdA==\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"document_id\":\"not duplicated\""
            + ",\"submission_timestamp\":\"2020-01-12T21:03:18.234567Z\"}"
            + ",\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{}" + ",\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{}" + ",\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"document_id\":\"strange timestamps\""
            + ",\"submission_timestamp\":\"2020-01-12T21:03:18.234567Z\"}"
            + ",\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"document_id\":\"foo\""
            + ",\"submission_timestamp\":\"2020-01-12T21:02:18.123456Z\"}"
            + ",\"payload\":\"dGVzdA==\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(DeduplicateByDocumentId.of()) //
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

}
