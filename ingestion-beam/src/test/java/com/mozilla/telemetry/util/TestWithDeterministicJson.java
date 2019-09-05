/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class TestWithDeterministicJson {

  /** Make serialization of Map deterministic for testing. */
  @BeforeClass
  public static void enableOrderMapEntriesByKeys() {
    Json.enableOrderMapEntriesByKeys();
  }

  /** Reset the ObjectMapper configuration changed for testing. */
  @AfterClass
  public static void disableOrderMapEntriesByKeys() {
    Json.disableOrderMapEntriesByKeys();
  }

  /**
   * Reserialize {@code data} to make json deterministic.
   */
  public static String sortJson(String data) {
    return Json.sortJson(data);
  }
}
