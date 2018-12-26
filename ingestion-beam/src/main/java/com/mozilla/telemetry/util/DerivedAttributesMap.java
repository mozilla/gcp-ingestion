/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import com.google.common.collect.ForwardingMap;
import java.util.Map;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Light wrapper for the attributes map of a PubsubMessage that supports retrieval of some
 * additional pseduo-attributes computed from attributes.
 *
 * <p>In particular, parses submission_date and submission_hour on demand based on the
 * submission_timestamp attribute if present.
 */
public class DerivedAttributesMap extends ForwardingMap<String, String> {

  /** Static factory for wrapping an attribute map. */
  public static Map<String, String> of(Map<String, String> m) {
    if (m == null) {
      return null;
    } else {
      return new DerivedAttributesMap(m);
    }
  }

  @Override
  public String get(Object key) {
    if (!(key instanceof String)) {
      throw new IllegalArgumentException("Key must be of type String");
    }
    String value = attributes.get(key);
    if (value != null) {
      return value;
    } else if ("submission_date".equals(key)) {
      return submissionTimestampSubstring(0, 10);
    } else if ("submission_hour".equals(key)) {
      return submissionTimestampSubstring(11, 13);
    }
    return null;
  }

  @Override
  public String getOrDefault(Object key, String defaultValue) {
    String value = get(key);
    return value == null ? defaultValue : value;
  }

  @Override
  public boolean containsKey(@Nullable Object key) {
    return (attributes.containsKey(key) || get(key) != null);
  }

  ///////////////////////

  private Map<String, String> attributes;

  @Override
  protected Map<String, String> delegate() {
    return attributes;
  }

  private DerivedAttributesMap(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  private String submissionTimestampSubstring(int beginIndex, int endIndex) {
    String timestamp = Optional.ofNullable(attributes.get("submission_timestamp")).orElse("");
    if (timestamp.length() >= endIndex) {
      return timestamp.substring(beginIndex, endIndex);
    } else {
      return null;
    }
  }

}
