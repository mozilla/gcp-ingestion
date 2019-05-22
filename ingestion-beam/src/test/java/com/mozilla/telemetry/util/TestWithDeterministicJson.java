/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class TestWithDeterministicJson {

  /** Make serialization of attributes map deterministic for these tests. */
  @BeforeClass
  public static void setUp() {
    Json.MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  /** Reset the ObjectMapper configurations we changed. */
  @AfterClass
  public static void tearDown() {
    Json.MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }
}
