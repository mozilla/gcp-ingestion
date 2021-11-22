package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.nio.charset.StandardCharsets;
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

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testNoop() {
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "test") //
        .put(Attribute.DOCUMENT_TYPE, "test") //
        .put(Attribute.DOCUMENT_VERSION, "1") //
        .put(Attribute.SUBMISSION_TIMESTAMP, "2021-01-01T12:00:00.123456Z") //
        .put(Attribute.GEO_CITY, "Toronto") //
        .build();
    PCollection<String> result = pipeline //
        .apply(Create.of(new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributes))) //
        .apply(SanitizeAttributes.of("schemas.tar.gz"))
        .apply(MapElements.into(TypeDescriptors.strings())
            .via(message -> message.getAttribute(Attribute.SUBMISSION_TIMESTAMP)));
    PAssert.that(result).containsInAnyOrder("2021-01-01T12:00:00.123456Z");
    pipeline.run();
  }

  @Test
  public void testSubmissionTimestampGranularity() {
    // Depends on custom doctype injected in bin/download-schemas
    Map<String, String> attributes = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, "test") //
        .put(Attribute.DOCUMENT_TYPE, "seconds") //
        .put(Attribute.DOCUMENT_VERSION, "1") //
        .put("submission_timestamp", "2021-01-01T12:00:00.123456Z") //
        .build();
    PCollection<String> result = pipeline //
        .apply(Create.of(new PubsubMessage("{}".getBytes(StandardCharsets.UTF_8), attributes))) //
        .apply(SanitizeAttributes.of("schemas.tar.gz"))
        .apply(MapElements.into(TypeDescriptors.strings())
            .via(message -> message.getAttribute(Attribute.SUBMISSION_TIMESTAMP)));
    PAssert.that(result).containsInAnyOrder("2021-01-01T12:00:00Z");
    pipeline.run();
  }
}
