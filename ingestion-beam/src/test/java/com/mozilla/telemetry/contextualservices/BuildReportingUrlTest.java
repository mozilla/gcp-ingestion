package com.mozilla.telemetry.contextualservices;

import org.junit.Test;

import static org.junit.Assert.assertThrows;

public class BuildReportingUrlTest {

  @Test
  public void testDuplicateQueryKeys() {
    assertThrows(
        BuildReportingUrl.InvalidUrlException.class,
        () -> new BuildReportingUrl("https://example.com?foo=bar&foo=bar")
    );
  }

}
