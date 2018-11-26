/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import java.io.IOException;
import org.junit.Test;

public class JsonTest {

  @Test
  public void instantiateJsonForCodeCoverage() {
    new Json();
  }

  @Test
  public void testReadTableRowSuceedsOnEmptyJsonObject() throws Exception {
    Json.readTableRow("{}".getBytes());
  }

  @Test(expected = IOException.class)
  public void testReadTableRowThrowsOnNull() throws Exception {
    Json.readTableRow(null);
  }

  @Test(expected = IOException.class)
  public void testReadTableRowThrowsOnEmptyArray() throws Exception {
    Json.readTableRow(new byte[] {});
  }

  @Test(expected = IOException.class)
  public void testReadTableRowThrowsOnNullJson() throws Exception {
    Json.readTableRow("null".getBytes());
  }

}
