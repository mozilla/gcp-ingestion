/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.util;

import com.mozilla.telemetry.ingestion.core.util.Time;
import java.time.Duration;
import java.util.Optional;

public class Env {

  private Env() {
  }

  public static Optional<String> optString(String key) {
    return Optional.ofNullable(System.getenv(key));
  }

  public static String getString(String key) {
    return optString(key)
        .orElseThrow(() -> new IllegalArgumentException("Missing required env var: " + key));
  }

  public static String getString(String key, String defaultValue) {
    return optString(key).orElse(defaultValue);
  }

  public static Integer getInt(String key, Integer defaultValue) {
    return optString(key).map(Integer::new).orElse(defaultValue);
  }

  public static Long getLong(String key, Long defaultValue) {
    return optString(key).map(Long::new).orElse(defaultValue);
  }

  public static Duration getDuration(String key, String defaultValue) {
    return Time.parseJavaDuration(getString(key, defaultValue));
  }
}
