/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.transform;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.io.BigQuery.Write.TableRow;
import com.mozilla.telemetry.ingestion.transform.PubsubMessageToTableRow.TableRowFormat;
import org.junit.Test;

public class PubsubMessageToTableRowTest {

  private static final PubsubMessageToTableRow TRANSFORM = new PubsubMessageToTableRow(
      "${document_namespace}_raw.${document_type}_v${document_version}", TableRowFormat.raw);

  @Test
  public void canTransformSingleMessage() {
    final TableRow actual = TRANSFORM
        .apply(PubsubMessage.newBuilder().setData(ByteString.copyFrom("test".getBytes()))
            .putAttributes("document_id", "id").putAttributes("document_namespace", "telemetry")
            .putAttributes("document_type", "main").putAttributes("document_version", "4").build());

    assertEquals("telemetry_raw", actual.tableId.getDataset());
    assertEquals("main_v4", actual.tableId.getTable());
    assertEquals(104, actual.byteSize);
    assertEquals("test", new String((byte[]) actual.content.remove("payload")));
    assertEquals(ImmutableMap.of("document_id", "id", "document_namespace", "telemetry",
        "document_type", "main", "document_version", "4"), actual.content);
  }

  @Test
  public void canHandleEmptyValues() {
    final TableRow actual = TRANSFORM
        .apply(PubsubMessage.newBuilder().putAttributes("document_namespace", "")
            .putAttributes("document_type", "").putAttributes("document_version", "").build());

    assertEquals("_raw", actual.tableId.getDataset());
    assertEquals("_v", actual.tableId.getTable());
    assertEquals(65, actual.byteSize);
    assertEquals("", new String((byte[]) actual.content.remove("payload")));
    assertEquals(
        ImmutableMap.of("document_namespace", "", "document_type", "", "document_version", ""),
        actual.content);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnMissingAttributes() {
    TRANSFORM.apply(PubsubMessage.newBuilder().build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnInvalidTable() {
    new PubsubMessageToTableRow("", TableRowFormat.raw).apply(PubsubMessage.newBuilder().build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnUnimplementedFormat() {
    new PubsubMessageToTableRow("dataset.table", TableRowFormat.decoded)
        .apply(PubsubMessage.newBuilder().build());
  }

  @Test(expected = NullPointerException.class)
  public void failsOnNullMessage() {
    TRANSFORM.apply(null);
  }
}
