/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink.io;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToJSONObject.Format;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.Before;
import org.junit.Test;

public class GcsWriteTest {

  private static final PubsubMessage EMPTY_MESSAGE = PubsubMessage.newBuilder().build();
  private static final int EMPTY_MESSAGE_SIZE = "{}\n".getBytes(StandardCharsets.UTF_8).length;
  private static final String BATCH_KEY = "bucket/prefix/";
  private static final int MAX_BYTES = 10;
  private static final int MAX_MESSAGES = 10;
  private static final Duration MAX_DELAY = Duration.ofMillis(100);

  private Storage storage;
  private Gcs.Write.Ndjson output;
  private WriteChannel writer;

  private CompletableFuture<Void> batchCloseHook(BlobInfo ignore) {
    return CompletableFuture.completedFuture(null);
  }

  @Before
  public void mockBigQueryResponse() {
    storage = mock(Storage.class);
    writer = mock(WriteChannel.class);
    when(storage.writer(any())).thenReturn(writer);
    output = new Gcs.Write.Ndjson(storage, MAX_BYTES, MAX_MESSAGES, MAX_DELAY, BATCH_KEY,
        Format.raw, this::batchCloseHook);
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
    output = new Gcs.Write.Ndjson(storage, MAX_BYTES, MAX_MESSAGES, Duration.ofMillis(0), BATCH_KEY,
        Format.raw, this::batchCloseHook);
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

  @Test(expected = UncheckedIOException.class)
  public void failsOnInsertErrors() throws Throwable {
    doThrow(new IOException()).when(writer).close();

    try {
      output.apply(EMPTY_MESSAGE).join();
    } catch (CompletionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnOversizedMessage() {
    output.apply(PubsubMessage.newBuilder().putAttributes("meta", "data").build());
  }
}
