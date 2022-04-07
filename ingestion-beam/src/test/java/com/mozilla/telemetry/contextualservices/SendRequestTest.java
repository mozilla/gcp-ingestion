package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Collections;
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

  private SponsoredInteraction.Builder getTestInteraction() {
    return SponsoredInteraction.builder().setInteractionType("click").setSource("topsite")
        .setFormFactor("phone").setContextId("1");
  }

  @Test
  public void testReportingDisabled() {
    MockWebServer server = new MockWebServer();

    SponsoredInteraction interaction = getTestInteraction()
        .setReportingUrl(server.url("/test").toString()).build();

    System.out.println(interaction);
    List<SponsoredInteraction> input = Collections.singletonList(interaction);

    pipeline.apply(Create.of(input)).apply(SendRequest.of(false, false));

    pipeline.run();

    Assert.assertEquals(0, server.getRequestCount());
  }

  @Test
  public void testRequestExceptionOnError() {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(500));
    server.enqueue(new MockResponse().setResponseCode(401));

    SponsoredInteraction interaction = getTestInteraction()
        .setReportingUrl(server.url("/test").toString()).build();

    List<SponsoredInteraction> input = ImmutableList.of(interaction, interaction);

    WithFailures.Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline
        .apply(Create.of(input)).apply(SendRequest.of(true, false));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(2, Iterables.size(messages));
      return null;
    });

    pipeline.run();

    Assert.assertEquals(2, server.getRequestCount());
  }

  @Test
  public void testSuccessfulSend() {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(204));

    SponsoredInteraction interaction = getTestInteraction()
        .setReportingUrl(server.url("/test").toString()).build();

    List<SponsoredInteraction> input = ImmutableList.of(interaction, interaction);

    WithFailures.Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline
        .apply(Create.of(input)).apply(SendRequest.of(true, false));

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

    SponsoredInteraction interaction = getTestInteraction()
        .setReportingUrl(server.url("/test").toString()).build();

    List<SponsoredInteraction> input = ImmutableList.of(interaction);

    WithFailures.Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline
        .apply(Create.of(input)).apply(SendRequest.of(true, false));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterables.size(messages));
      return null;
    });

    pipeline.run();

    Assert.assertEquals(1, server.getRequestCount());
  }

  @Test
  public void testLogReportingUrls() {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200));

    String requestUrl = server.url("/test").toString();
    SponsoredInteraction interaction = getTestInteraction().setReportingUrl(requestUrl).build();

    List<SponsoredInteraction> input = ImmutableList.of(interaction);

    WithFailures.Result<PCollection<SponsoredInteraction>, PubsubMessage> result = pipeline
        .apply(Create.of(input)).apply(SendRequest.of(true, true));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterables.size(messages));

      PubsubMessage message = Iterables.get(messages, 0);
      String errorMessage = message.getAttribute("error_message");

      Assert.assertTrue(
          errorMessage.contains(SendRequest.RequestContentException.class.getSimpleName()));
      Assert.assertTrue(errorMessage.contains(requestUrl));

      return null;
    });

    pipeline.run();

    Assert.assertEquals(1, server.getRequestCount());
  }
}
