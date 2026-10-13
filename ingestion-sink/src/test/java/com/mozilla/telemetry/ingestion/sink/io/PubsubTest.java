package com.mozilla.telemetry.ingestion.sink.io;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BatchException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.junit.Test;
import org.slf4j.Logger;

public class PubsubTest {

  @Test
  public void canLogBatchDescription() {
    final Logger logger = mock(Logger.class);
    final Error cause = new OutOfMemoryError("test");
    final CompletableFuture<Void> result = new CompletableFuture<>();
    result.completeExceptionally(BatchException.of(cause, 2, "gs://bucket/blob.ndjson"));
    Pubsub.getConsumer(logger, Function.identity(), message -> result)
        .receiveMessage(PubsubMessage.newBuilder().build(), mock(AckReplyConsumer.class));
    verify(logger).error("failed to deliver 2 messages in batch gs://bucket/blob.ndjson", cause);
  }
}
