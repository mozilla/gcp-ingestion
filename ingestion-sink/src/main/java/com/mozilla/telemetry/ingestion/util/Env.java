/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.util;

import java.time.Duration;
import java.util.Optional;

public class Env {

  private Env() {
  }

  public static String getString(String key) {
    String value = getString(key, null);
    if (value == null) {
      throw new IllegalArgumentException("Missing required env var: " + key);
    }
    return value;
  }

  public static String getString(String key, String defaultValue) {
    return Optional.ofNullable(System.getenv(key)).orElse(defaultValue);
  }

  public static Optional<String> optString(String key) {
    return Optional.ofNullable(System.getenv(key));
  }

  public static Integer getInteger(String key, Integer defaultValue) {
    return optString(key).map(Integer::parseInt).orElse(defaultValue);
  }

  public static Duration getDuration(String key, String defaultValue) {
    return Duration.parse(getString(key, defaultValue));
  }
}
