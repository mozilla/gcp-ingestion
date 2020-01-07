package com.mozilla.telemetry.ingestion.sink.util;

import com.mozilla.telemetry.ingestion.sink.config.SinkConfig;
import com.mozilla.telemetry.ingestion.sink.io.Pubsub;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility for testing an unbounded {@link SinkConfig} with a termination condition.
 */
public class BoundedSink extends SinkConfig {

  /**
   * Run async until {@code messageCount} messages have been delivered.
   */
  public static CompletableFuture<Void> runAsync(int messageCount) {
    final Output output = getOutput();
    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicReference<Pubsub.Read> input = new AtomicReference<>();
    input.set(getInput(output.via(message -> output.apply(message).thenApplyAsync(v -> {
      final int currentMessages = counter.incrementAndGet();
      if (currentMessages >= messageCount) {
        input.get().subscriber.stopAsync();
      }
      return v;
    }))));
    return CompletableFuture.completedFuture(input.get()).thenAcceptAsync(Pubsub.Read::run);
  }

  /**
   * Wait up to {@code timeout} seconds for {@code messageCount} messages to be delivered.
   */
  public static void run(int messageCount, int timeout) {
    run(runAsync(messageCount), timeout);
  }

  /**
   * Wait up to {@code timeout} seconds for {@code future} to complete.
   */
  public static void run(CompletableFuture<Void> future, int timeout) {
    try {
      synchronized (future) {
        future.wait(timeout);
      }
    } catch (InterruptedException e) {
      future.cancel(true);
    }
    try {
      future.join();
    } catch (CancellationException e) {
      throw new RuntimeException("Failed to deliver messages in " + timeout + " seconds", e);
    }
  }
}
