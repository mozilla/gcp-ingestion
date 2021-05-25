package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

  @Test
  public void testReportingDisabled() {
    MockWebServer server = new MockWebServer();

    Map<String, String> attributes = Collections.singletonMap(Attribute.REPORTING_URL,
        server.url("/test").toString());

    List<PubsubMessage> input = Collections
        .singletonList(new PubsubMessage(new byte[0], attributes));

    pipeline.apply(Create.of(input))
        .apply(SendRequest.of(pipeline.newProvider(false), pipeline.newProvider(false)));

    pipeline.run();

    Assert.assertEquals(0, server.getRequestCount());
  }

  @Test
  public void testRequestExceptionOnError() {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(500));
    server.enqueue(new MockResponse().setResponseCode(401));

    Map<String, String> attributes = Collections.singletonMap(Attribute.REPORTING_URL,
        server.url("/test").toString());

    List<PubsubMessage> input = ImmutableList.of(new PubsubMessage(new byte[0], attributes),
        new PubsubMessage(new byte[0], attributes));

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline
        .apply(Create.of(input))
        .apply(SendRequest.of(pipeline.newProvider(true), pipeline.newProvider(false)));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(2, Iterators.size(messages.iterator()));
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

    Map<String, String> attributes = Collections.singletonMap(Attribute.REPORTING_URL,
        server.url("/test").toString());

    List<PubsubMessage> input = ImmutableList.of(new PubsubMessage(new byte[0], attributes),
        new PubsubMessage(new byte[0], attributes));

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline
        .apply(Create.of(input))
        .apply(SendRequest.of(pipeline.newProvider(true), pipeline.newProvider(false)));

    PAssert.that(result.output()).satisfies(messages -> {
      Assert.assertEquals(2, Iterators.size(messages.iterator()));
      return null;
    });

    pipeline.run();

    Assert.assertEquals(2, server.getRequestCount());
  }

  @Test
  public void testRedirectError() {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(302));

    Map<String, String> attributes = Collections.singletonMap(Attribute.REPORTING_URL,
        server.url("/test").toString());

    List<PubsubMessage> input = Collections
        .singletonList(new PubsubMessage(new byte[0], attributes));

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> result = pipeline
        .apply(Create.of(input))
        .apply(SendRequest.of(pipeline.newProvider(true), pipeline.newProvider(false)));

    PAssert.that(result.failures()).satisfies(messages -> {
      Assert.assertEquals(1, Iterators.size(messages.iterator()));
      return null;
    });

    pipeline.run();

    Assert.assertEquals(1, server.getRequestCount());
  }

  @Test
  public void testLogReportingUrls() {
    // TODO
  }
}
