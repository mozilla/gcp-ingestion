package com.mozilla.telemetry.ingestion.sink.io;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BatchException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class WriteWithErrorsTest {

  @Test
  public void canRetryBatchException() {
    final AtomicInteger attempt = new AtomicInteger(0);
    final List<String> errors = new LinkedList<>();

    new WriteWithErrors(msg -> {
      if (attempt.incrementAndGet() == 1) {
        throw BatchException.of(new RuntimeException(), 0);
      }
      throw new RuntimeException();
    }, msg -> {
      errors.add(msg.getMessageId());
      return CompletableFuture.completedFuture(null);
    }, 2).apply(PubsubMessage.newBuilder().setMessageId("1").build()).join();
    assertEquals(3, attempt.get());
    assertEquals(ImmutableList.of("1"), errors);
  }
}
