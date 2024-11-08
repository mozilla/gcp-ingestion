package com.mozilla.telemetry.contextualservices;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertThrows;

public class BuildReportingUrlTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testDuplicateQueryKeys() {
    assertThrows(
        BuildReportingUrl.InvalidUrlException.class,
        () -> new BuildReportingUrl("https://example.com?foo=bar&foo=bar")
    );
  }
}
