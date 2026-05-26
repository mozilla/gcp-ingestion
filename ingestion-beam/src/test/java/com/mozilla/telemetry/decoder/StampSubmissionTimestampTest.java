package com.mozilla.telemetry.decoder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
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

  @Test
  public void stampsTimestampWhenMissing() {
    PubsubMessage input = new PubsubMessage(BODY,
        ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, "ads-backend", Attribute.DOCUMENT_TYPE,
            "request-stats", Attribute.DOCUMENT_VERSION, "1", Attribute.DOCUMENT_ID,
            "6c702690-02a7-4e81-b4cb-550a1ce10a22"));

    TestStream<PubsubMessage> stream = TestStream.create(PubsubMessageWithAttributesCoder.of())
        .advanceWatermarkTo(PUBLISH_TIME).addElements(TimestampedValue.of(input, PUBLISH_TIME))
        .advanceWatermarkToInfinity();

    PCollection<PubsubMessage> output = pipeline.apply(stream).apply(StampSubmissionTimestamp.of());

    PAssert.thatSingleton(output).satisfies(message -> {
      assertEquals("submission_timestamp must be stamped from element timestamp",
          PUBLISH_TIME.toString(), message.getAttribute(Attribute.SUBMISSION_TIMESTAMP));
      // Non-timestamp attributes pass through unchanged.
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
    PubsubMessage input = new PubsubMessage(BODY,
        ImmutableMap.of(Attribute.DOCUMENT_NAMESPACE, "ads-backend", Attribute.DOCUMENT_TYPE,
            "request-stats", Attribute.DOCUMENT_VERSION, "1", Attribute.DOCUMENT_ID,
            "6c702690-02a7-4e81-b4cb-550a1ce10a22", Attribute.SUBMISSION_TIMESTAMP, preStamped));

    TestStream<PubsubMessage> stream = TestStream.create(PubsubMessageWithAttributesCoder.of())
        .advanceWatermarkTo(PUBLISH_TIME).addElements(TimestampedValue.of(input, PUBLISH_TIME))
        .advanceWatermarkToInfinity();

    PCollection<PubsubMessage> output = pipeline.apply(stream).apply(StampSubmissionTimestamp.of());

    PAssert.thatSingleton(output).satisfies(message -> {
      // Pre-existing submission_timestamp must be preserved (reprocessing / Edge case).
      assertEquals("submission_timestamp must not be overwritten", preStamped,
          message.getAttribute(Attribute.SUBMISSION_TIMESTAMP));
      assertArrayEquals("Body bytes must be preserved exactly", BODY, message.getPayload());
      return null;
    });

    pipeline.run();
  }
}
