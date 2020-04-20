package com.mozilla.telemetry.ingestion.sink.io;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import org.junit.Before;
import org.junit.Test;

public class GcsWriteTest {

  private static final PubsubMessage EMPTY_MESSAGE = PubsubMessage.newBuilder().build();
  private static final int EMPTY_MESSAGE_SIZE = "{}\n".getBytes(StandardCharsets.UTF_8).length;
  private static final String BATCH_KEY = "bucket/prefix/";
  private static final PubsubMessageToTemplatedString BATCH_KEY_TEMPLATE = //
      PubsubMessageToTemplatedString.of(BATCH_KEY);
  private static final int MAX_BYTES = 10;
  private static final int MAX_MESSAGES = 10;
  private static final Duration MAX_DELAY = Duration.ofMillis(100);

  private Storage storage;
  private Gcs.Write.Ndjson output;

  private CompletableFuture<Void> batchCloseHook(BlobInfo ignore) {
    return CompletableFuture.completedFuture(null);
  }

  /** Prepare a mock BQ response. */
  @Before
  public void mockBigQueryResponse() {
    storage = mock(Storage.class);
    output = new Gcs.Write.Ndjson(storage, MAX_BYTES, MAX_MESSAGES, MAX_DELAY, BATCH_KEY_TEMPLATE,
        ForkJoinPool.commonPool(), PubsubMessageToObjectNode.Raw.of(), this::batchCloseHook);
  }

  @Test
  public void canReturnSuccess() {
    output.apply(EMPTY_MESSAGE).join();
    BlobInfo blobInfo = ((Gcs.Write.Batch) output.batches.get(BATCH_KEY)).blobInfo;
    assertEquals("bucket", blobInfo.getBucket());
    assertThat(blobInfo.getName(), matchesPattern("prefix/[a-f0-9-]{36}\\.ndjson"));
  }

  @Test
  public void canSendWithNoDelay() {
    output = new Gcs.Write.Ndjson(storage, MAX_BYTES, MAX_MESSAGES, Duration.ofMillis(0),
        BATCH_KEY_TEMPLATE, ForkJoinPool.commonPool(), PubsubMessageToObjectNode.Raw.of(),
        this::batchCloseHook);
    output.apply(EMPTY_MESSAGE).join();
    assertEquals(1, output.batches.get(BATCH_KEY).size);
    assertEquals(EMPTY_MESSAGE_SIZE, output.batches.get(BATCH_KEY).byteSize);
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
    for (int i = 0; i < Math.ceil((MAX_BYTES + 1) / EMPTY_MESSAGE_SIZE); i++) {
      output.apply(EMPTY_MESSAGE);
    }
    assertThat((int) output.batches.get(BATCH_KEY).byteSize, lessThanOrEqualTo(MAX_BYTES));
  }

  @Test
  public void failsOnInsertErrors() {
    final Throwable expect = new RuntimeException("fail");
    doThrow(expect).when(storage).create(any(BlobInfo.class), any(byte[].class), any());
    Throwable cause = null;
    try {
      output.apply(EMPTY_MESSAGE).join();
    } catch (CompletionException e) {
      cause = e.getCause();
    }
    assertEquals(expect, cause);
  }

  @Test
  public void canHandleOversizeMessage() {
    output.apply(PubsubMessage.newBuilder().putAttributes("meta", "data").build()).join();
  }
}
