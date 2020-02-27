package com.mozilla.telemetry.decoder;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class ParsePayloadTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSampleId() {
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");
    ParsePayload transform = ParsePayload.of(schemasLocation);
    assertEquals(67L, transform.calculateSampleId("2907648d-711b-4e9f-94b5-52a2b40a44b1"));
    assertEquals(17L, transform.calculateSampleId("90210716-99f8-0a4f-8119-9bfc16cd68a3"));
    assertEquals(0L, transform.calculateSampleId(""));
  }

  @Test
  public void testUppercaseUuidNormalization() {
    final String lowercaseUuid = "90210716-99f8-0a4f-8119-9bfc16cd68a3";
    final String lowercaseNormalizedUuid = ParsePayload.normalizeUuid(lowercaseUuid);
    assertEquals(lowercaseNormalizedUuid, lowercaseUuid);

    final String uppercaseUuid = "90210716-99F8-0A4F-8119-9BFC16CD68A3";
    final String uppercaseNormalizedUuid = ParsePayload.normalizeUuid(uppercaseUuid);
    assertEquals(uppercaseNormalizedUuid, lowercaseUuid);
  }

  @Test
  public void testOutput() {
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");
    final List<String> input = Arrays.asList("{}", "{\"id\":null}", "[]", "{",
        "{\"clientId\":\"2907648d-711b-4e9f-94b5-52a2b40a44b1\"}",
        "{\"clientId\":\"2907648D-711B-4E9F-94B5-52A2B40A44B1\"}",
        "{\"impression_id\":\"{2907648d-711b-4e9f-94b5-52a2b40a44b1}\"}", "{\"client_id\":\"n/a\"}",
        "{\"client_id\":\"n/a\",\"impression_id\":\"{2907648d-711b-4e9f-94b5-52a2b40a44b1}\"}");
    Result<PCollection<PubsubMessage>, PubsubMessage> output = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.text.decode())
        .apply("AddAttributes", MapElements.into(TypeDescriptor.of(PubsubMessage.class))
            .via(element -> new PubsubMessage(element.getPayload(), ImmutableMap.of(
                "document_namespace", "test", "document_type", "test", "document_version", "1"))))
        .apply(ParsePayload.of(schemasLocation));

    final List<String> expectedMain = Arrays.asList("{}", "{\"id\":null}",
        "{\"clientId\":\"2907648d-711b-4e9f-94b5-52a2b40a44b1\"}",
        "{\"clientId\":\"2907648d-711b-4e9f-94b5-52a2b40a44b1\"}",
        "{\"impression_id\":\"{2907648d-711b-4e9f-94b5-52a2b40a44b1}\"}", "{\"client_id\":\"n/a\"}",
        "{\"client_id\":\"n/a\",\"impression_id\":\"{2907648d-711b-4e9f-94b5-52a2b40a44b1}\"}");
    final PCollection<String> main = output.output().apply("encodeTextMain",
        OutputFileFormat.text.encode());
    PAssert.that(main).containsInAnyOrder(expectedMain);

    final List<String> expectedAttributes = Arrays.asList(
        "{\"document_namespace\":\"test\",\"document_version\":\"1\",\"document_type\":\"test\"}",
        "{\"document_namespace\":\"test\",\"document_version\":\"1\",\"document_type\":\"test\"}",
        "{\"document_namespace\":\"test\",\"document_version\":\"1\""
            + ",\"client_id\":\"2907648d-711b-4e9f-94b5-52a2b40a44b1\""
            + ",\"document_type\":\"test\",\"sample_id\":\"67\"}",
        "{\"document_namespace\":\"test\",\"document_version\":\"1\""
            + ",\"client_id\":\"2907648d-711b-4e9f-94b5-52a2b40a44b1\""
            + ",\"document_type\":\"test\",\"sample_id\":\"67\"}",
        "{\"document_namespace\":\"test\",\"document_version\":\"1\""
            + ",\"document_type\":\"test\",\"sample_id\":\"67\"}",
        "{\"document_namespace\":\"test\",\"document_version\":\"1\""
            + ",\"document_type\":\"test\"}",
        "{\"document_namespace\":\"test\",\"document_version\":\"1\""
            + ",\"document_type\":\"test\",\"sample_id\":\"67\"}");
    final PCollection<String> attributes = output.output()
        .apply(MapElements.into(TypeDescriptors.strings()).via(m -> {
          try {
            return Json.asString(m.getAttributeMap());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }));
    PAssert.that(attributes).containsInAnyOrder(expectedAttributes);

    final List<String> expectedError = Arrays.asList("[]", "{");
    final PCollection<String> error = output.failures().apply("encodeTextError",
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

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(ParsePayload.of(schemasLocation));

    PCollection<String> exceptions = result.failures().apply(MapElements
        .into(TypeDescriptors.strings()).via(message -> message.getAttribute("exception_class")));

    PAssert.that(result.output()).empty();
    PAssert.that(exceptions).containsInAnyOrder("java.lang.RuntimeException",
        "com.mozilla.telemetry.ingestion.core.schema.SchemaNotFoundException",
        "com.mozilla.telemetry.ingestion.core.schema.SchemaNotFoundException",
        "com.mozilla.telemetry.ingestion.core.schema.SchemaNotFoundException");

    pipeline.run();
  }

  @Test
  public void testVersionInPayload() {
    ValueProvider<String> schemasLocation = pipeline.newProvider("schemas.tar.gz");

    // printf '{"version":4}' | base64 -> eyJ2ZXJzaW9uIjo0fQ==
    String input = "{\"attributeMap\":" //
        + "{\"document_namespace\":\"telemetry\"" //
        + ",\"document_id\":\"2c3a0767-d84a-4d02-8a92-fa54a3376049\"" //
        + ",\"document_type\":\"main\"" //
        + "},\"payload\":\"eyJ2ZXJzaW9uIjo0fQ==\"}";

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline.apply(Create.of(input))
        .apply(InputFileFormat.json.decode()).apply(ParsePayload.of(schemasLocation));

    PCollection<String> exceptions = result.failures().apply(MapElements
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

    String input = "{\"id\":null,\"document_id\":\"2c3a0767-d84a-4d02-8a92-fa54a3376049\""
        + ",\"metadata\":{\"document_namespace\":\"test\",\"document_type\":\"test\""
        + ",\"document_version\":\"1\",\"geo\":{\"country\":\"FI\"}}}";

    Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.text.decode()) //
        .apply(ParsePayload.of(schemasLocation));

    PAssert.that(result.failures()).empty();

    final PCollection<Integer> attributeCounts = result.output().apply(MapElements
        .into(TypeDescriptors.integers()).via(message -> message.getAttributeMap().size()));
    PAssert.thatSingleton(attributeCounts).isEqualTo(5);

    final String expectedMain = "{\"id\":null}";
    final PCollection<String> main = result.output() //
        .apply("encodeTextMain", OutputFileFormat.text.encode());
    PAssert.thatSingleton(main).isEqualTo(expectedMain);

    pipeline.run();
  }
}
