package com.mozilla.telemetry.amplitude;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;


public class SendRequestTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private AmplitudeEvent.Builder getTestAmplitudeEvent() {
    return AmplitudeEvent.builder().setUserId("test").setEventType("category.event")
        .setPlatform("firefox-desktop").setTime(1738620582500L);
  }

  private AmplitudeEvent.Builder getTestAmplitudeEventWithExtras() {
    return AmplitudeEvent.builder().setUserId("test").setEventType("category.event")
        .setPlatform("firefox-desktop").setTime(1738620582500L)
        .setEventExtras("{\"test\": 12, \"foo\": true}");
  }

  @Test
  public void testReportingDisabled() {
    MockWebServer server = new MockWebServer();

    AmplitudeEvent event = getTestAmplitudeEvent().build();
    List<Iterable<AmplitudeEvent>> input = ImmutableList.of(ImmutableList.of(event));

    pipeline.apply(Create.of(input)).apply(SendRequest.of("key", false, 10));

    pipeline.run();

    Assert.assertEquals(0, server.getRequestCount());
  }

  @Test
  public void testRequestExceptionOnError() {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(500));
    server.enqueue(new MockResponse().setResponseCode(401));

    AmplitudeEvent event = getTestAmplitudeEvent().build();

    List<Iterable<AmplitudeEvent>> input = ImmutableList.of(ImmutableList.of(event),
        ImmutableList.of(event));

    WithFailures.Result<PCollection<Iterable<AmplitudeEvent>>, PubsubMessage> result = pipeline
        .apply(Create.of(input)).apply(SendRequest.of("key", true, 10, server.url("/batch").toString()));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(2, Iterables.size(messages));
      return null;
    });

    pipeline.run();

    Assert.assertEquals(2, server.getRequestCount());
  }

  @Test
  public void testSuccessfulSend() throws JsonProcessingException {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(204));

    AmplitudeEvent event = getTestAmplitudeEventWithExtras().build();

    List<Iterable<AmplitudeEvent>> input = ImmutableList.of(ImmutableList.of(event),
        ImmutableList.of(event));

    WithFailures.Result<PCollection<Iterable<AmplitudeEvent>>, PubsubMessage> result = pipeline
        .apply(Create.of(input)).apply(SendRequest.of("key", true, 1, server.url("/batch").toString()));

    PAssert.that(result.output()).satisfies(messages -> {
      Assert.assertEquals(2, Iterables.size(messages));
      return null;
    });

    pipeline.run();

    Assert.assertEquals(2, server.getRequestCount());
  }

  @Test
  public void testSuccessfulBatchSend() throws JsonProcessingException {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(204));

    AmplitudeEvent event = getTestAmplitudeEventWithExtras().build();

    List<Iterable<AmplitudeEvent>> input = ImmutableList.of(ImmutableList.of(event, event),
        ImmutableList.of(event, event));

    WithFailures.Result<PCollection<Iterable<AmplitudeEvent>>, PubsubMessage> result = pipeline
        .apply(Create.of(input)).apply(SendRequest.of("key", true, 2, server.url("/batch").toString()));

    PAssert.that(result.output()).satisfies(messages -> {
      Assert.assertEquals(2, Iterables.size(messages));
      return null;
    });

    pipeline.run();

    Assert.assertEquals(2, server.getRequestCount());
  }

  @Test
  public void testRedirectError() {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(302));

    AmplitudeEvent event = getTestAmplitudeEventWithExtras().build();

    List<Iterable<AmplitudeEvent>> input = ImmutableList.of(ImmutableList.of(event));

    WithFailures.Result<PCollection<Iterable<AmplitudeEvent>>, PubsubMessage> result = pipeline
        .apply(Create.of(input)).apply(SendRequest.of("key", true, 1, server.url("/batch").toString()));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterables.size(messages));
      return null;
    });

    pipeline.run();

    Assert.assertEquals(1, server.getRequestCount());
  }
}
