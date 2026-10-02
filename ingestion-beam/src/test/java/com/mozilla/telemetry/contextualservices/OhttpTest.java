package com.mozilla.telemetry.contextualservices;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class OhttpTest {

  private static Map<String, String> attributes(String namespace, String docType,
      String userAgentAttribute) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
        .put(Attribute.DOCUMENT_NAMESPACE, namespace).put(Attribute.DOCUMENT_TYPE, docType);
    if (userAgentAttribute != null) {
      builder.put(userAgentAttribute, "value");
    }
    return builder.build();
  }

  @Test
  public void testSuggestPingsWithoutUserAgentAreOhttp() {
    Assert.assertTrue(Ohttp.isOhttpSuggest(attributes("firefox-desktop", "quick-suggest", null)));
    Assert.assertTrue(Ohttp.isOhttpSuggest(attributes("org-mozilla-firefox", "fx-suggest", null)));
    Assert.assertTrue(
        Ohttp.isOhttpSuggest(attributes("org-mozilla-ios-firefox", "fx-suggest", null)));
  }

  @Test
  public void testAnyUserAgentAttributeMeansSubmittedDirectly() {
    for (String userAgentAttribute : new String[] { Attribute.USER_AGENT_BROWSER,
        Attribute.USER_AGENT_OS, Attribute.USER_AGENT_VERSION }) {
      Assert.assertFalse(userAgentAttribute,
          Ohttp.isOhttpSuggest(attributes("firefox-desktop", "quick-suggest", userAgentAttribute)));
      Assert.assertFalse(userAgentAttribute, Ohttp
          .isOhttpSuggest(attributes("org-mozilla-firefox", "fx-suggest", userAgentAttribute)));
    }
  }

  @Test
  public void testOtherDocTypesAreNotOhttp() {
    // Not submitted via OHTTP, so a missing user agent is not a sign of OHTTP
    Assert.assertFalse(Ohttp.isOhttpSuggest(attributes("firefox-desktop", "top-sites", null)));
    Assert.assertFalse(Ohttp.isOhttpSuggest(attributes("firefox-desktop", "search-with", null)));
    Assert.assertFalse(
        Ohttp.isOhttpSuggest(attributes("org-mozilla-firefox", "topsites-impression", null)));
    Assert.assertFalse(
        Ohttp.isOhttpSuggest(attributes("contextual-services", "quicksuggest-impression", null)));
  }
}
