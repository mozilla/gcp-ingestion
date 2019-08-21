/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.Before;
import org.junit.Test;

public class BigQueryTest {

  private com.google.cloud.bigquery.BigQuery bigquery;
  private BigQuery.Write output;
  private InsertAllResponse response;

  @Before
  public void mockBigQueryResponse() {
    bigquery = mock(com.google.cloud.bigquery.BigQuery.class);
    response = mock(InsertAllResponse.class);
    when(bigquery.insertAll(any())).thenReturn(response);
    when(response.getErrorsFor(anyLong())).thenReturn(ImmutableList.of());
    output = new BigQuery.Write(bigquery, 10, 10, Duration.ofMillis(100));
  }

  @Test
  public void canReturnSuccess() {
    BigQuery.Write.TableRow row = new BigQuery.Write.TableRow(TableId.of("", ""), 0,
        Optional.empty(), ImmutableMap.of("", ""));
    assertEquals(row, output.apply(row).join());
  }

  @Test
  public void canSendWithNoDelay() {
    output = new BigQuery.Write(bigquery, 1, 1, Duration.ofMillis(0));
    output.apply(
        new BigQuery.Write.TableRow(TableId.of("", ""), 0, Optional.empty(), ImmutableMap.of()));
    assertEquals(1, output.batches.get(TableId.of("", "")).builder.build().getRows().size());
  }

  @Test
  public void canBatchMessages() {
    for (int i = 0; i < 2; i++) {
      output.apply(
          new BigQuery.Write.TableRow(TableId.of("", ""), 0, Optional.empty(), ImmutableMap.of()));
    }
    assertEquals(2, output.batches.get(TableId.of("", "")).builder.build().getRows().size());
  }

  @Test
  public void canLimitBatchMessageCount() {
    for (int i = 0; i < 11; i++) {
      output.apply(
          new BigQuery.Write.TableRow(TableId.of("", ""), 0, Optional.empty(), ImmutableMap.of()));
    }
    assertTrue(output.batches.get(TableId.of("", "")).builder.build().getRows().size() < 10);
  }

  @Test
  public void canLimitBatchByteSize() {
    for (int i = 0; i < 2; i++) {
      output.apply(
          new BigQuery.Write.TableRow(TableId.of("", ""), 6, Optional.empty(), ImmutableMap.of()));
    }
    assertTrue(output.batches.get(TableId.of("", "")).builder.build().getRows().size() < 2);
  }

  @Test(expected = BigQuery.WriteErrors.class)
  public void failsOnInsertErrors() throws Throwable {
    when(response.getErrorsFor(0)).thenReturn(ImmutableList.of(new BigQueryError("", "", "")));

    try {
      output.apply(new BigQuery.Write.TableRow(TableId.of("", ""), 0, Optional.empty(),
          ImmutableMap.of("", ""))).join();
    } catch (CompletionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnOversizedMessage() {
    output.apply(new BigQuery.Write.TableRow(TableId.of("", ""), 11, Optional.empty(), null));
  }
}
