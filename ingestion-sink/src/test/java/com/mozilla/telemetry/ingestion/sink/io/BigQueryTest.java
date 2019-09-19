/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.io;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode.Format;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.Before;
import org.junit.Test;

public class BigQueryTest {

  private static final PubsubMessage EMPTY_MESSAGE = PubsubMessage.newBuilder().build();
  private static final int MAX_BYTES = 100;
  private static final int MAX_MESSAGES = 10;
  private static final Duration MAX_DELAY = Duration.ofMillis(100);
  private static final Duration NO_DELAY = Duration.ofMillis(0);
  private static final String BATCH_KEY_TEMPLATE = "_._";
  private static final TableId BATCH_KEY = TableId.of("_", "_");

  private com.google.cloud.bigquery.BigQuery bigQuery;
  private BigQuery.Write output;
  private InsertAllResponse response;

  @Before
  public void mockBigQueryResponse() {
    bigQuery = mock(com.google.cloud.bigquery.BigQuery.class);
    response = mock(InsertAllResponse.class);
    when(bigQuery.insertAll(any())).thenReturn(response);
    when(response.getErrorsFor(anyLong())).thenReturn(ImmutableList.of());
    output = new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, MAX_DELAY, BATCH_KEY_TEMPLATE,
        Format.raw);
  }

  @Test
  public void canReturnSuccess() {
    output.apply(EMPTY_MESSAGE).join();
  }

  @Test
  public void canSendWithNoDelay() {
    output = new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, Duration.ofMillis(0),
        BATCH_KEY_TEMPLATE, Format.raw);
    output.apply(EMPTY_MESSAGE);
    assertEquals(1, output.batches.get(BATCH_KEY).size);
  }

  @Test
  public void canBatchMessages() {
    for (int i = 0; i < 2; i++) {
      output.apply(EMPTY_MESSAGE);
    }
    assertEquals(2, output.batches.get(BATCH_KEY).size);
  }

  @Test
  public void canLimitBatchMessageCount() {
    for (int i = 0; i < (MAX_MESSAGES + 1); i++) {
      output.apply(EMPTY_MESSAGE);
    }
    assertThat(output.batches.get(BATCH_KEY).size, lessThanOrEqualTo(MAX_MESSAGES));
  }

  @Test
  public void canLimitBatchByteSize() {
    PubsubMessage input = PubsubMessage.newBuilder().putAttributes("meta", "data").build();
    for (int i = 0; i < Math.ceil((MAX_BYTES + 1) / input.getSerializedSize()); i++) {
      output.apply(input);
    }
    assertThat((int) output.batches.get(BATCH_KEY).byteSize, lessThanOrEqualTo(MAX_BYTES));
  }

  @Test(expected = BigQuery.WriteErrors.class)
  public void failsOnInsertErrors() throws Throwable {
    when(response.getErrorsFor(0)).thenReturn(ImmutableList.of(new BigQueryError("", "", "")));

    try {
      output.apply(EMPTY_MESSAGE).join();
    } catch (CompletionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnOversizedMessage() {
    output.apply(
        PubsubMessage.newBuilder().putAttributes("error", Strings.repeat(".", MAX_BYTES)).build());
  }

  @Test
  public void canHandleProjectInTableId() {
    output = new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, NO_DELAY,
        "project.dataset.table", Format.raw);
    output.apply(EMPTY_MESSAGE).join();
    assertNotNull(output.batches.get(TableId.of("project", "dataset", "table")));
  }

  @Test
  public void canHandleDocumentId() {
    output = new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, NO_DELAY, BATCH_KEY_TEMPLATE,
        Format.raw);
    output.apply(PubsubMessage.newBuilder().putAttributes("document_id", "id").build()).join();
    List<InsertAllRequest.RowToInsert> rows = ((BigQuery.Write.Batch) output.batches
        .get(BATCH_KEY)).builder.build().getRows();

    assertEquals("id", rows.get(0).getId());
    assertEquals(ImmutableMap.of("document_id", "id"), rows.get(0).getContent());
    assertEquals(20, output.batches.get(BATCH_KEY).byteSize);
  }

  @Test
  public void canHandleDynamicTableId() {
    output = new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, NO_DELAY, "${dataset}.${table}",
        Format.raw);
    output.apply(PubsubMessage.newBuilder().putAttributes("dataset", "dataset")
        .putAttributes("table", "table").build()).join();
    assertNotNull(output.batches.get(TableId.of("dataset", "table")));
  }

  @Test
  public void canHandleDynamicTableIdWithEmptyValues() {
    output = new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, NO_DELAY,
        "${dataset}_.${table}_", Format.raw);
    output.apply(
        PubsubMessage.newBuilder().putAttributes("dataset", "").putAttributes("table", "").build())
        .join();
    assertNotNull(output.batches.get(BATCH_KEY));
  }

  @Test
  public void canHandleDynamicTableIdWithDefaults() {
    output = new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, NO_DELAY,
        "${dataset:-dataset}.${table:-table}", Format.raw);
    output.apply(EMPTY_MESSAGE).join();
    assertNotNull(output.batches.get(TableId.of("dataset", "table")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnMissingAttributes() {
    new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, NO_DELAY, "${dataset}.${table}",
        Format.raw).apply(EMPTY_MESSAGE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnInvalidTable() {
    new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, NO_DELAY, "", Format.raw)
        .apply(EMPTY_MESSAGE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnUnimplementedFormat() {
    new BigQuery.Write(bigQuery, MAX_BYTES, MAX_MESSAGES, NO_DELAY, BATCH_KEY_TEMPLATE,
        Format.payload).apply(EMPTY_MESSAGE);
  }

  @Test(expected = NullPointerException.class)
  public void failsOnNullMessage() {
    output.apply(null);
  }
}
