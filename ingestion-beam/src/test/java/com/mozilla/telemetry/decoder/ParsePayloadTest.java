/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.WithErrors;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class ParsePayloadTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");
    final List<String> input = Arrays.asList("{}", "{\"id\":null}", "[]", "{");
    WithErrors.Result<PCollection<PubsubMessage>> output = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode()).output()
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via(element -> new PubsubMessage(element.getPayload(), ImmutableMap.of(
                "document_namespace", "test", "document_type", "test", "document_version", "1"))))
        .apply(ParsePayload.of(schemasLocation));

    final List<String> expectedMain = Arrays.asList("{}", "{\"id\":null}");
    final PCollection<String> main = output.output().apply("encodeTextMain",
        OutputFileFormat.text.encode());
    PAssert.that(main).containsInAnyOrder(expectedMain);

    final List<String> expectedError = Arrays.asList("[]", "{");
    final PCollection<String> error = output.errors().apply("encodeTextError",
        OutputFileFormat.text.encode());
    PAssert.that(error).containsInAnyOrder(expectedError);

    pipeline.run();
  }

  @Test
  public void testErrors() {
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");
    final List<String> input = Arrays.asList(
        // non-json payload
        "{\"attributeMap\":" + "{\"document_namespace\":\"eng-workflow\""
            + ",\"document_version\":\"1\""
            + ",\"document_id\":\"2c3a0767-d84a-4d02-8a92-fa54a3376049\""
            + ",\"document_type\":\"hgpush\"" + "},\"payload\":\"\"}",
        // incomplete attributes
        "{\"attributeMap\":{\"app_name\":\"Firefox\"" + ",\"app_version\":\"61.0a1\""
            + ",\"document_type\":\"main\"},\"payload\":\"e30K\"}",
        "{\"attributeMap\":{},\"payload\":\"e30K\"}",
        "{\"attributeMap\":null,\"payload\":\"e30K\"}");

    WithErrors.Result<PCollection<PubsubMessage>> result = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(ParsePayload.of(schemasLocation));

    PCollection<String> exceptions = result.errors().apply(MapElements
        .into(TypeDescriptors.strings()).via(message -> message.getAttribute("exception_class")));

    PAssert.that(result.output()).empty();
    PAssert.that(exceptions).containsInAnyOrder("java.io.IOException",
        "com.mozilla.telemetry.schemas.SchemaNotFoundException",
        "com.mozilla.telemetry.schemas.SchemaNotFoundException",
        "com.mozilla.telemetry.schemas.SchemaNotFoundException");

    pipeline.run();
  }

  @Test
  public void testVersionInPayload() {
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");

    // printf '{"version":4}' | base64 -> eyJ2ZXJzaW9uIjo0fQ==
    String input = "{\"attributeMap\":" + "{\"document_namespace\":\"telemetry\""
        + ",\"document_id\":\"2c3a0767-d84a-4d02-8a92-fa54a3376049\""
        + ",\"document_type\":\"main\"" + "},\"payload\":\"eyJ2ZXJzaW9uIjo0fQ==\"}";

    WithErrors.Result<PCollection<PubsubMessage>> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.json.decode()).output().apply(ParsePayload.of(schemasLocation));

    PCollection<String> exceptions = result.errors().apply(MapElements
        .into(TypeDescriptors.strings()).via(message -> message.getAttribute("exception_class")));

    PAssert.that(result.output()).empty();

    // If we get a ValidationException here, it means we successfully extracted version from
    // the payload and found a valid schema; we expect the payload to not validate.
    PAssert.that(exceptions).containsInAnyOrder("org.everit.json.schema.ValidationException");

    pipeline.run();
  }

  @Test
  public void testMetadataInPayload() {
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");

    String input = "{\"id\":null,\"metadata\":"
        + "{\"document_namespace\":\"test\",\"document_type\":\"test\",\"document_version\":1}}";

    WithErrors.Result<PCollection<PubsubMessage>> result = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.text.decode()).output() //
        .apply(ParsePayload.of(schemasLocation));

    PAssert.that(result.errors()).empty();

    final List<String> expectedMain = Arrays.asList("{\"id\":null}");
    final PCollection<String> main = result.output().apply("encodeTextMain",
        OutputFileFormat.text.encode());

    final PCollection<Integer> attributeCounts = result.output().apply(MapElements
        .into(TypeDescriptors.integers()).via(message -> message.getAttributeMap().size()));
    PAssert.that(attributeCounts).containsInAnyOrder(Arrays.asList(3));

    pipeline.run();
  }

}
