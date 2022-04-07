package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.Builder;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;

public class LabelClickSpikesTest {

  private SponsoredInteraction.Builder getTestInteraction() {
    return SponsoredInteraction.builder()
            .interaction("click")
            .source("topsite")
            .form("phone")
            .contextID("1");
  }

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSetsClickStatus() {

    SponsoredInteraction interaction = getTestInteraction()
            .contextID("a")
            .reporterURL("https://test.com")
            .build();
    Builder<SponsoredInteraction> eventBuilder = TestStream.create(SerializableCoder.of(SponsoredInteraction.class));

    // We add 20 messages each only a second apart. The first 10 should saturate the timestamp
    // state, then the final 10 should be marked as suspicious via click-status.
    for (int i = 1; i <= 20; i++) {
      eventBuilder = eventBuilder.advanceProcessingTime(Duration.standardSeconds(i))
          .addElements(interaction);
    }

    TestStream<SponsoredInteraction> createEvents = eventBuilder.advanceWatermarkToInfinity();

    PCollection<SponsoredInteraction> result = pipeline.apply(createEvents) //
        .apply(WithKeys.of("a")) //
        .apply(LabelClickSpikes.of(10, Duration.standardMinutes(3))).apply(Values.create());

    PAssert.that(result).satisfies(iter -> {
      int size = Iterables.size(iter);
      assert size == 20 : "Expected 20 messages, but found " + size;
      return null;
    });

    PAssert.that(result).satisfies(iter -> {
      long countWithStatus = StreamSupport.stream(iter.spliterator(), false) //
          .filter(m -> m.reporterURL().contains("click-status=65")) //
          .count();
      assert countWithStatus == 10 : ("Expected 10 messages with click-status, but found "
          + countWithStatus);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testIgnoresSlowClickRate() {
    SponsoredInteraction interaction = getTestInteraction()
            .contextID("a")
            .reporterURL("https://test.com")
            .build();
    Builder<SponsoredInteraction> eventBuilder = TestStream.create(SerializableCoder.of(SponsoredInteraction.class));

    // These 20 messages arrive one minute apart from each other, so old timestamps should
    // expire before we hit the click threshold. These should have no click status set.
    for (int i = 1; i <= 20; i++) {
      eventBuilder = eventBuilder.advanceProcessingTime(Duration.standardMinutes(i))
          .addElements(interaction);
    }

    TestStream<SponsoredInteraction> createEvents = eventBuilder.advanceWatermarkToInfinity();

    PCollection<SponsoredInteraction> result = pipeline.apply(createEvents) //
        .apply(WithKeys.of("a")) //
        .apply(LabelClickSpikes.of(10, Duration.standardMinutes(3))).apply(Values.create());

    PAssert.that(result).satisfies(iter -> {
      int size = Iterables.size(iter);
      assert size == 20 : "Expected 20 messages, but found " + size;
      return null;
    });

    PAssert.that(result).satisfies(iter -> {
      long countWithStatus = StreamSupport.stream(iter.spliterator(), false) //
          .filter(m -> m.reporterURL().contains("click-status=65")) //
          .count();
      assert countWithStatus == 0 : ("Expected 0 messages with click-status, but found "
          + countWithStatus);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFlushesState() {
    SponsoredInteraction interaction = getTestInteraction()
            .contextID("a")
            .reporterURL("https://test.com")
            .build();
    SponsoredInteraction[] interactions = new SponsoredInteraction[8];
    Arrays.fill(interactions, interaction);
    TestStream<SponsoredInteraction> createEvents = TestStream
        .create(SerializableCoder.of(SponsoredInteraction.class))
        .addElements(interactions[0], Arrays.copyOfRange(interactions, 1, 8))
        .advanceProcessingTime(Duration.standardMinutes(4))
        .addElements(interactions[0], Arrays.copyOfRange(interactions, 1, 8)) //
        .advanceWatermarkToInfinity();

    PCollection<SponsoredInteraction> result = pipeline.apply(createEvents) //
        .apply(WithKeys.of("a")) //
        .apply(LabelClickSpikes.of(10, Duration.standardMinutes(3))) //
        .apply(Values.create());

    PAssert.that(result).satisfies(iter -> {
      int size = Iterables.size(iter);
      assert size == 16 : "Expected 16 messages, but found " + size;
      return null;
    });

    PAssert.that(result).satisfies(iter -> {
      long countWithStatus = StreamSupport.stream(iter.spliterator(), false) //
          .filter(m -> m.reporterURL().contains("click-status=65")) //
          .count();
      assert countWithStatus == 0 : ("Expected 0 messages with click_status, but found "
          + countWithStatus);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

}
