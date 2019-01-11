/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertEquals;

import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.model.TableRow;
import org.junit.Test;

public class PubsubMessageToTableRowTest {

  @Test
  public void testBuildTableRow() throws Exception {
    String input = "{\"metadata\":{\"somenum\":3,\"submission_timestamp\":\"2018-01-01T12:13:14\"}"
        + ",\"otherkey\":\"hi\"}";
    String expected = "{\"metadata\":{\"somenum\":3}"
        + ",\"otherkey\":\"hi\",\"submission_timestamp\":\"2018-01-01T12:13:14\"}";
    TableRow tableRow = PubsubMessageToTableRow.buildTableRow(input.getBytes());
    tableRow.setFactory(new JacksonFactory());
    assertEquals(expected, tableRow.toString());
  }

}
