/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.Test;

public class BigQueryTest {

  @Test
  public void canBatchMessages() {
    BigQuery.Write output = new BigQuery.Write(null);
    for (int i = 0; i < 2; i++) {
      output.apply(
          new BigQuery.Write.TableRow(TableId.of("", ""), 0, Optional.empty(), ImmutableMap.of()));
    }
    assertEquals(2, output.batches.get(TableId.of("", "")).builder.build().getRows().size());
  }

  @Test
  public void canLimitBatchMessageCount() {
    BigQuery.Write output = new BigQuery.Write(null);
    for (int i = 0; i < 10_0001; i++) {
      output.apply(
          new BigQuery.Write.TableRow(TableId.of("", ""), 0, Optional.empty(), ImmutableMap.of()));
    }
    assertTrue(output.batches.get(TableId.of("", "")).builder.build().getRows().size() < 10_000);
  }

  @Test
  public void canLimitBatchByteSize() {
    BigQuery.Write output = new BigQuery.Write(null);
    for (int i = 0; i < 10; i++) {
      output.apply(new BigQuery.Write.TableRow(TableId.of("", ""), 1_000_000, Optional.empty(),
          ImmutableMap.of()));
    }
    assertTrue(output.batches.get(TableId.of("", "")).builder.build().getRows().size() < 10);
  }

  @Test
  public void canReturnRowOnSuccess() throws InterruptedException {
    com.google.cloud.bigquery.BigQuery bigquery = mock(com.google.cloud.bigquery.BigQuery.class);
    InsertAllResponse response = mock(InsertAllResponse.class);

    when(bigquery.insertAll(any())).thenReturn(response);
    when(response.getErrorsFor(0)).thenReturn(ImmutableList.of());

    BigQuery.Write.TableRow row = new BigQuery.Write.TableRow(TableId.of("", ""), 0,
        Optional.empty(), ImmutableMap.of("", ""));
    assertEquals(row, new BigQuery.Write(bigquery).apply(row).join());
  }

  @Test(expected = BigQuery.WriteErrors.class)
  public void failsOnInsertErrors() throws Throwable {
    com.google.cloud.bigquery.BigQuery bigquery = mock(com.google.cloud.bigquery.BigQuery.class);
    InsertAllResponse response = mock(InsertAllResponse.class);
    // BigQueryError is a final class, so mocking it requires 'mock-maker-inline' in
    // src/test/resources/mockito-extensions/org.mockito.plugins.MockMaker
    BigQueryError error = mock(BigQueryError.class);

    when(bigquery.insertAll(any())).thenReturn(response);
    when(response.getErrorsFor(0)).thenReturn(ImmutableList.of(error));
    BigQuery.Write output = new BigQuery.Write(bigquery);

    try {
      output.apply(new BigQuery.Write.TableRow(TableId.of("", ""), 0, Optional.empty(),
          ImmutableMap.of("", ""))).join();
    } catch (CompletionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnOversizedMessage() {
    new BigQuery.Write(null)
        .apply(new BigQuery.Write.TableRow(TableId.of("", ""), 10_000_000, Optional.empty(), null));
  }
}
