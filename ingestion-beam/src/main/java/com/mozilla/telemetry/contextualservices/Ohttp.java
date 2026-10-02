package com.mozilla.telemetry.contextualservices;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Identify suggest pings submitted via OHTTP.
 *
 * <p>OHTTP submissions reach the pipeline from the OHTTP gateway, so they carry no User-Agent
 * header and their IP-derived geo is the gateway's rather than the client's. The decoder drops the
 * raw header after parsing, so a missing header is identified by all the parsed user agent
 * attributes being absent.
 *
 * <p>This is a bit brittle, in the future it would be better to have the decoder attach an
 * explicit signal that indicates OHTTP.
 */
class Ohttp {

  /**
   * Whether this is a suggest ping submitted via OHTTP: desktop quick-suggest or mobile fx-suggest
   * without a User-Agent header. Other doc types, such as top-sites, are not submitted via OHTTP.
   */
  static boolean isOhttpSuggest(Map<String, String> attributes) {
    String docType = attributes.get(Attribute.DOCUMENT_TYPE);
    return ("quick-suggest".equals(docType) || "fx-suggest".equals(docType))
        && !hasUserAgent(attributes);
  }

  /** Whether any of the parsed user agent attributes is present. */
  static boolean hasUserAgent(Map<String, String> attributes) {
    return Stream
        .of(Attribute.USER_AGENT_BROWSER, Attribute.USER_AGENT_OS, Attribute.USER_AGENT_VERSION)
        .map(attributes::get).anyMatch(Objects::nonNull);
  }
}
