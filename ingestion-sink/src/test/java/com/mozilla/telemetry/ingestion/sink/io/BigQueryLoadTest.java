package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import org.junit.Before;
import org.junit.Test;

public class BigQueryLoadTest {

  private Storage storage;
  private com.google.cloud.bigquery.BigQuery bigQuery;
  private Job job;
  private JobStatus jobStatus;

  /** Prepare a mock BQ response. */
  @Before
  public void setupMock() throws InterruptedException {
    storage = mock(Storage.class);
    bigQuery = mock(com.google.cloud.bigquery.BigQuery.class);
    job = mock(Job.class);
    jobStatus = mock(JobStatus.class);
    when(bigQuery.create(any(JobInfo.class), any())).thenReturn(job);
    when(job.waitFor(any())).thenReturn(job);
    when(job.getStatus()).thenReturn(jobStatus);
  }

  private Optional<BigQuery.BigQueryErrors> load() {
    final Blob blob = mock(Blob.class);
    final String name = "OUTPUT_TABLE=dataset.table/x";
    when(blob.getName()).thenReturn(name);
    when(blob.getBlobId()).thenReturn(BlobId.of("bucket", name));
    when(blob.getSize()).thenReturn(1L);
    try {
      new BigQuery.Load(bigQuery, storage, 0, 0, Duration.ZERO, ForkJoinPool.commonPool(),
          BigQuery.Load.Delete.onSuccess).apply(blob).join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof BigQuery.BigQueryErrors) {
        return Optional.of((BigQuery.BigQueryErrors) e.getCause());
      }
    }
    return Optional.empty();
  }

  @Test
  public void canLoad() {
    // mock success status
    when(jobStatus.getError()).thenReturn(null);
    when(jobStatus.getExecutionErrors()).thenReturn(null);
    assertEquals(Optional.empty(), load());
  }

  @Test
  public void canIgnoreEmptyExecutionErrors() {
    // mock alternate success status
    when(jobStatus.getError()).thenReturn(null);
    when(jobStatus.getExecutionErrors()).thenReturn(ImmutableList.of());
    assertEquals(Optional.empty(), load());
  }

  @Test
  public void canDetectError() {
    // mock error result
    BigQueryError error = new BigQueryError("reason", "location", "message");
    when(jobStatus.getError()).thenReturn(error);
    when(jobStatus.getExecutionErrors()).thenReturn(null);
    assertEquals(Optional.of(ImmutableList.of(error)), load().map(e -> e.errors));
  }

  @Test
  public void canDetectExecutionErrors() {
    // mock partial error result
    BigQueryError error = new BigQueryError("reason", "location", "message");
    when(jobStatus.getError()).thenReturn(null);
    when(jobStatus.getExecutionErrors()).thenReturn(ImmutableList.of(error));
    assertEquals(Optional.of(ImmutableList.of(error)), load().map(e -> e.errors));
  }
}
