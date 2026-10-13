package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
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

  private static final JobId JOB_ID = JobId.of("project", "job-id");

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
    when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
    when(job.waitFor()).thenReturn(job);
    when(job.getStatus()).thenReturn(jobStatus);
  }

  private Optional<BigQuery.BigQueryErrors> load() {
    final Blob blob = mock(Blob.class);
    final String name = "OUTPUT_TABLE=dataset.table/x";
    when(blob.getName()).thenReturn(name);
    when(blob.getBlobId()).thenReturn(BlobId.of("bucket", name));
    when(blob.getSize()).thenReturn(1L);
    try {
      new BigQuery.Load(bigQuery, storage, 0, 0, Duration.ZERO, ForkJoinPool.commonPool(), false,
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

  @Test
  public void canDescribeFailedJob() throws InterruptedException {
    // Job.waitFor throws when the job finished with an error
    BigQueryError error = new BigQueryError("notFound", "location", "Not found: URI gs://x");
    BigQueryException exception = new BigQueryException(ImmutableList.of(error));
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.waitFor()).thenThrow(exception);
    BigQuery.BigQueryErrors errors = load().get();
    assertEquals(ImmutableList.of(error), errors.errors);
    assertEquals("waiting for load job " + JOB_ID + " into dataset.table with 1 files failed: ["
        + error + "]", errors.getMessage());
    assertSame(exception, errors.getCause());
  }

  @Test
  public void canDescribeWaitFailureWithoutErrors() throws InterruptedException {
    // Job.waitFor also throws when checking the job's status fails, e.g. on a timeout
    BigQueryException exception = new BigQueryException(503, "backend error");
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.waitFor()).thenThrow(exception);
    BigQuery.BigQueryErrors errors = load().get();
    assertEquals(ImmutableList.of(), errors.errors);
    assertEquals(
        "waiting for load job " + JOB_ID + " into dataset.table with 1 files failed: backend error",
        errors.getMessage());
    assertSame(exception, errors.getCause());
  }

  @Test
  public void canDescribeCreateFailure() {
    BigQueryError error = new BigQueryError("rateLimitExceeded", "location", "too many jobs");
    BigQueryException exception = new BigQueryException(ImmutableList.of(error));
    when(bigQuery.create(any(JobInfo.class))).thenThrow(exception);
    BigQuery.BigQueryErrors errors = load().get();
    assertEquals(ImmutableList.of(error), errors.errors);
    assertEquals("creating load job into dataset.table with 1 files failed: [" + error + "]",
        errors.getMessage());
    assertSame(exception, errors.getCause());
  }

  @Test
  public void canDescribeMissingJob() throws InterruptedException {
    // Job.waitFor returns null when the job is not found
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.waitFor()).thenReturn(null);
    assertEquals(
        "load job " + JOB_ID + " into dataset.table with 1 files was not found while waiting for it to "
            + "finish",
        load().get().getMessage());
  }
}
