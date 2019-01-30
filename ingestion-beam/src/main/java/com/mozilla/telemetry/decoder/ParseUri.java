/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

public class ParseUri extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  public static ParseUri of() {
    return INSTANCE;
  }

  ////////

  private static class InvalidUriException extends Exception {

    InvalidUriException() {
      super();
    }

    InvalidUriException(String message) {
      super(message);
    }
  }

  private static class NullUriException extends InvalidUriException {
  }

  private ParseUri() {
  }

  private static final ParseUri INSTANCE = new ParseUri();

  public static final String TELEMETRY_URI_PREFIX = "/submit/telemetry/";
  public static final String[] TELEMETRY_URI_SUFFIX_ELEMENTS = new String[] { "document_id",
      "document_type", "app_name", "app_version", "app_update_channel", "app_build_id" };
  public static final String GENERIC_URI_PREFIX = "/submit/";
  public static final String[] GENERIC_URI_SUFFIX_ELEMENTS = new String[] { "document_namespace",
      "document_type", "document_version", "document_id" };

  private static Map<String, String> zip(String[] keys, String[] values)
      throws InvalidUriException {
    Map<String, String> map = new HashMap<>();
    if (keys.length != values.length) {
      throw new InvalidUriException(
          String.format("Found %d more path elements in the URI than expected for this endpoint",
              values.length - keys.length));
    }
    for (int i = 0; i < keys.length; i++) {
      map.put(keys[i], values[i]);
    }
    return map;
  }

  @Override
  protected PubsubMessage processElement(PubsubMessage message) throws InvalidUriException {
    message = PubsubConstraints.ensureNonNull(message);
    // Copy attributes
    final Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

    // parse uri based on prefix
    final String uri = attributes.get("uri");
    if (uri == null) {
      throw new NullUriException();
    } else if (uri.startsWith(TELEMETRY_URI_PREFIX)) {
      // We don't yet have access to the version field, so we delay populating the document_version
      // attribute until the ParsePayload step where we have map-like access to the JSON content.
      attributes.put("document_namespace", "telemetry");
      attributes.putAll(zip(TELEMETRY_URI_SUFFIX_ELEMENTS,
          uri.substring(TELEMETRY_URI_PREFIX.length()).split("/")));
    } else if (uri.startsWith(GENERIC_URI_PREFIX)) {
      attributes.putAll(
          zip(GENERIC_URI_SUFFIX_ELEMENTS, uri.substring(GENERIC_URI_PREFIX.length()).split("/")));
    } else {
      throw new InvalidUriException("Unknown URI prefix");
    }
    return new PubsubMessage(message.getPayload(), attributes);
  }
}
