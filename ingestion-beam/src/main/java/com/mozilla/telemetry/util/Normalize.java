/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import com.google.common.collect.ImmutableList;
import java.util.List;

public class Normalize {

  private static final String BETA = "beta";
  private static final String NIGHTLY = "nightly";
  private static final String OTHER = "Other";
  private static final List<String> CHANNELS = ImmutableList.of("release", "esr", BETA, "aurora",
      NIGHTLY);

  /**
   * Normalize a channel string to one of the five canonical release channels or "Other".
   *
   * <p>Reimplementation of https://github.com/mozilla-services/lua_sandbox_extensions/blob/sprint2018-6/moz_telemetry/modules/moz_telemetry/normalize.lua#L167-L178
   */
  public static String channel(String name) {
    if (CHANNELS.contains(name)) {
      return name;
    } else if (name == null) {
      return OTHER;
    } else if (name.startsWith("nightly-cck-")) {
      return "nightly";
    } else if (name.startsWith(BETA)) {
      return BETA;
    } else {
      return OTHER;
    }
  }
}
