/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
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

  @Test
  public void testReadBigQuerySchema() throws Exception {
    Schema schema = Json.readBigQuerySchema(
        "[{\"mode\":\"NULLABLE\",\"name\":\"document_id\",\"type\": \"STRING\"}]".getBytes());
    assertEquals(LegacySQLTypeName.STRING, schema.getFields().get(0).getType());
  }

}
