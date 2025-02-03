package com.mozilla.telemetry.amplitude;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class SendRequestTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private AmplitudeEvent.Builder getTestAmplitudeEvent() {
    return AmplitudeEvent.builder().setUserId("test").setEventType("category.event")
        .setTime(1738620582500l);
  }

  @Test
  public void testReportingDisabled() {
    MockWebServer server = new MockWebServer();

    Map<String, String> attributes = Collections.singletonMap("test", "test");

    AmplitudeEvent event = getTestAmplitudeEvent().build();
    List<AmplitudeEvent> input = ImmutableList.of(event);

    pipeline.apply(Create.of(input));

    pipeline.run();

    Assert.assertEquals(0, server.getRequestCount());
  }
}
