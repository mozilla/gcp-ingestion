package com.mozilla.telemetry.decoder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class StampSubmissionTimestampTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private static final byte[] BODY = "{\"metrics\":{\"string\":{\"event.name\":\"login\"}}}"
      .getBytes(StandardCharsets.UTF_8);
  private static final Instant PUBLISH_TIME = Instant.parse("2026-01-15T12:34:56.789Z");

  private static final Map<String, String> ALL_REQUIRED = ImmutableMap.of(
      Attribute.DOCUMENT_NAMESPACE, "ads-backend", Attribute.DOCUMENT_TYPE, "request-stats",
      Attribute.DOCUMENT_VERSION, "1", Attribute.DOCUMENT_ID,
      "6c702690-02a7-4e81-b4cb-550a1ce10a22");

  private WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> applyTransform(
      PubsubMessage input) {
    TestStream<PubsubMessage> stream = TestStream.create(PubsubMessageWithAttributesCoder.of())
        .advanceWatermarkTo(PUBLISH_TIME).addElements(TimestampedValue.of(input, PUBLISH_TIME))
        .advanceWatermarkToInfinity();
    return pipeline.apply(stream).apply(StampSubmissionTimestamp.of());
  }

  @Test
  public void stampsTimestampWhenMissing() {
    PubsubMessage input = new PubsubMessage(BODY, ALL_REQUIRED);

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = applyTransform(input);

    PAssert.that(result.failures()).empty();
    PAssert.thatSingleton(result.output()).satisfies(message -> {
      assertEquals("submission_timestamp must be stamped from element timestamp",
          PUBLISH_TIME.toString(), message.getAttribute(Attribute.SUBMISSION_TIMESTAMP));
      assertEquals("ads-backend", message.getAttribute(Attribute.DOCUMENT_NAMESPACE));
      assertEquals("request-stats", message.getAttribute(Attribute.DOCUMENT_TYPE));
      assertEquals("1", message.getAttribute(Attribute.DOCUMENT_VERSION));
      assertEquals("6c702690-02a7-4e81-b4cb-550a1ce10a22",
          message.getAttribute(Attribute.DOCUMENT_ID));
      assertArrayEquals("Body bytes must be preserved exactly", BODY, message.getPayload());
      return null;
    });

    pipeline.run();
  }

  @Test
  public void preservesExistingTimestamp() {
    String preStamped = "2025-12-01T00:00:00.000Z";
    Map<String, String> attrs = new HashMap<>(ALL_REQUIRED);
    attrs.put(Attribute.SUBMISSION_TIMESTAMP, preStamped);
    PubsubMessage input = new PubsubMessage(BODY, attrs);

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = applyTransform(input);

    PAssert.that(result.failures()).empty();
    PAssert.thatSingleton(result.output()).satisfies(message -> {
      assertEquals("submission_timestamp must not be overwritten", preStamped,
          message.getAttribute(Attribute.SUBMISSION_TIMESTAMP));
      assertArrayEquals("Body bytes must be preserved exactly", BODY, message.getPayload());
      return null;
    });

    pipeline.run();
  }

  private void assertMissingAttributeIsRejected(String attribute) {
    Map<String, String> attrs = new HashMap<>(ALL_REQUIRED);
    attrs.remove(attribute);
    PubsubMessage input = new PubsubMessage(BODY, attrs);

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = applyTransform(input);

    PAssert.that(result.output()).empty();
    PAssert.thatSingleton(result.failures()).satisfies(message -> {
      assertArrayEquals("Body must be preserved on failure", BODY, message.getPayload());
      assertEquals("StampSubmissionTimestamp", message.getAttribute("error_type"));
      String errorMessage = message.getAttribute("error_message");
      assertTrue("error_message must reference the missing attribute (" + attribute + "), got: "
          + errorMessage, errorMessage.contains(attribute));
      return null;
    });

    pipeline.run();
  }

  @Test
  public void routesMissingDocumentIdToFailures() {
    assertMissingAttributeIsRejected(Attribute.DOCUMENT_ID);
  }

  @Test
  public void routesMissingDocumentNamespaceToFailures() {
    assertMissingAttributeIsRejected(Attribute.DOCUMENT_NAMESPACE);
  }

  @Test
  public void routesMissingDocumentTypeToFailures() {
    assertMissingAttributeIsRejected(Attribute.DOCUMENT_TYPE);
  }

  @Test
  public void routesMissingDocumentVersionToFailures() {
    assertMissingAttributeIsRejected(Attribute.DOCUMENT_VERSION);
  }

  @Test
  public void routesEmptyDocumentIdToFailures() {
    Map<String, String> attrs = new HashMap<>(ALL_REQUIRED);
    attrs.put(Attribute.DOCUMENT_ID, "");
    PubsubMessage input = new PubsubMessage(BODY, attrs);

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = applyTransform(input);

    PAssert.that(result.output()).empty();
    PAssert.thatSingleton(result.failures()).satisfies(message -> {
      assertEquals("StampSubmissionTimestamp", message.getAttribute("error_type"));
      return null;
    });

    pipeline.run();
  }
}
