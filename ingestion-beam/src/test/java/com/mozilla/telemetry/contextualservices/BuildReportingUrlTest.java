package com.mozilla.telemetry.contextualservices;

import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class BuildReportingUrlTest {

  @Test
  public void testDuplicateQueryKeys() {
    assertThrows(BuildReportingUrl.InvalidUrlException.class,
        () -> new BuildReportingUrl("https://example.com?foo=bar&foo=bar"));
  }

}
