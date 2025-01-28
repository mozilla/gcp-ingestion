package com.mozilla.telemetry.amplitude;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class SendRequestTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testReportingDisabled() {
    MockWebServer server = new MockWebServer();

    Map<String, String> attributes = Collections.singletonMap("test", "test");

    List<PubsubMessage> input = Collections
        .singletonList(new PubsubMessage(new byte[0], attributes));

    pipeline.apply(Create.of(input)).apply(SendRequest.of(false, "http://test.com"));

    pipeline.run();

    Assert.assertEquals(0, server.getRequestCount());
  }
}
