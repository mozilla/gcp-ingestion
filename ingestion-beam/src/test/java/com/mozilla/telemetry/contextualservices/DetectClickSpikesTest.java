package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.Arrays;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.Builder;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class DetectClickSpikesTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSetsClickStatus() {
    ImmutableMap<String, String> attributes = ImmutableMap.of(Attribute.CONTEXT_ID, "a", //
        Attribute.REPORTING_URL, "https://test.com");
    PubsubMessage message = new PubsubMessage(new byte[] {}, attributes);
    PubsubMessage[] messages = new PubsubMessage[19];
    Arrays.fill(messages, message);
    Builder<PubsubMessage> eventBuilder = TestStream.create(PubsubMessageWithAttributesCoder.of());

    // These first 20 messages arrive one minute apart from each other, so old timestamps should
    // expire before we hit the click threshold. These should have no click status set.
    for (int i = 1; i <= 20; i++) {
      eventBuilder = eventBuilder
          .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(i)))
          .addElements(message);
    }

    // Now, we add 20 messages each only a second apart. The first 10 should saturate the timestamp
    // state, then the final 10 should be marked as suspicious via click-status.
    for (int i = 1; i <= 20; i++) {
      eventBuilder = eventBuilder
          .advanceWatermarkTo(
              new Instant(0L).plus(Duration.standardMinutes(30)).plus(Duration.standardSeconds(i)))
          .addElements(message);
    }

    TestStream<PubsubMessage> createEvents = eventBuilder.advanceWatermarkToInfinity();

    PCollection<PubsubMessage> result = pipeline.apply(createEvents) //
        .apply(WithKeys.of("a")) //
        .apply(DetectClickSpikes.of(10, Duration.standardMinutes(3))).apply(Values.create());

    PAssert.that(result).satisfies(iter -> {
      int size = Iterables.size(iter);
      assert size == 40 : "Expected 40 messages, but found " + size;
      return null;
    });

    PAssert.that(result).satisfies(iter -> {
      long countWithStatus = StreamSupport.stream(iter.spliterator(), false) //
          .filter(m -> m.getAttribute(Attribute.REPORTING_URL).contains("click-status=64")) //
          .count();
      assert countWithStatus == 10 : ("Expected 10 messages with click-status, but found "
          + countWithStatus);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFlushesState() {
    ImmutableMap<String, String> attributes = ImmutableMap.of(Attribute.CONTEXT_ID, "a", //
        Attribute.REPORTING_URL, "https://test.com");
    PubsubMessage[] messages = new PubsubMessage[8];
    Arrays.fill(messages, new PubsubMessage(new byte[] {}, attributes));
    TestStream<PubsubMessage> createEvents = TestStream
        .create(PubsubMessageWithAttributesCoder.of())
        .addElements(messages[0], Arrays.copyOfRange(messages, 1, 8))
        .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(4)))
        .addElements(messages[0], Arrays.copyOfRange(messages, 1, 8)) //
        .advanceWatermarkToInfinity();

    PCollection<PubsubMessage> result = pipeline.apply(createEvents) //
        .apply(WithKeys.of("a")) //
        .apply(DetectClickSpikes.of(10, Duration.standardMinutes(3))) //
        .apply(Values.create());

    PAssert.that(result).satisfies(iter -> {
      int size = Iterables.size(iter);
      assert size == 16 : "Expected 16 messages, but found " + size;
      return null;
    });

    PAssert.that(result).satisfies(iter -> {
      long countWithStatus = StreamSupport.stream(iter.spliterator(), false) //
          .filter(m -> m.getAttribute(Attribute.REPORTING_URL).contains("click-status=64")) //
          .count();
      assert countWithStatus == 0 : ("Expected 0 messages with click_status, but found "
          + countWithStatus);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

}
