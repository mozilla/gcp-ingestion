package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class SanitizeAttributesTest extends TestWithDeterministicJson {

  static final byte[] EMPTY_JSON = "{}".getBytes(StandardCharsets.UTF_8);

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testNoop() {
    Map<String, String> inputAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "test") //
        .put(Attribute.DOCUMENT_TYPE, "test") //
        .put(Attribute.DOCUMENT_VERSION, "1") //
        .put(Attribute.SUBMISSION_TIMESTAMP, "2021-01-01T12:00:00.123456Z") //
        .put(Attribute.GEO_CITY, "Toronto") //
        .build();

    // Output should match the input exactly.
    Map<String, String> expected = new HashMap<>(inputAttributes);

    PCollection<Map<String, String>> result = pipeline //
        .apply(Create.of(new PubsubMessage(EMPTY_JSON, inputAttributes))) //
        .apply(SanitizeAttributes.of("schemas.tar.gz"))
        .apply(MapElements
            .into(TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via(PubsubMessage::getAttributeMap));
    PAssert.that(result).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  public void testSubmissionTimestampGranularity() {
    // Depends on custom doctype injected in bin/download-schemas
    Map<String, String> inputAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "test") //
        .put(Attribute.DOCUMENT_TYPE, "seconds") //
        .put(Attribute.DOCUMENT_VERSION, "1") //
        .put(Attribute.SUBMISSION_TIMESTAMP, "2021-01-01T12:00:00.123456Z") //
        .put(Attribute.GEO_CITY, "Toronto") //
        .build();

    Map<String, String> expected = new HashMap<>(inputAttributes);
    expected.put(Attribute.SUBMISSION_TIMESTAMP, "2021-01-01T12:00:00Z");

    PCollection<Map<String, String>> result = pipeline //
        .apply(Create.of(new PubsubMessage(EMPTY_JSON, inputAttributes))) //
        .apply(SanitizeAttributes.of("schemas.tar.gz"))
        .apply(MapElements
            .into(TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via(PubsubMessage::getAttributeMap));
    PAssert.that(result).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  public void testOverrideAttributes() {
    // Depends on custom doctype injected in bin/download-schemas
    Map<String, String> inputAttributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "test") //
        .put(Attribute.DOCUMENT_TYPE, "attribute-overrides") //
        .put(Attribute.DOCUMENT_VERSION, "1") //
        .put(Attribute.SUBMISSION_TIMESTAMP, "2021-01-01T12:00:00.123456Z") //
        .put(Attribute.GEO_CITY, "Toronto") //
        .put(Attribute.NORMALIZED_CHANNEL, "bizarreCustomValue") //
        .build();

    Map<String, String> expected = new HashMap<>(inputAttributes);
    expected.remove(Attribute.GEO_CITY);
    expected.put(Attribute.NORMALIZED_CHANNEL, "nightly");
    expected.put("new_attr", "injected_value");

    PCollection<Map<String, String>> result = pipeline //
        .apply(Create.of(new PubsubMessage(EMPTY_JSON, inputAttributes))) //
        .apply(SanitizeAttributes.of("schemas.tar.gz"))
        .apply(MapElements
            .into(TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via(PubsubMessage::getAttributeMap));
    PAssert.that(result).containsInAnyOrder(expected);
    pipeline.run();
  }
}
