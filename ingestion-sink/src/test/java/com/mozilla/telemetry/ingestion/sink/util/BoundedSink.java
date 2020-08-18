package com.mozilla.telemetry.ingestion.sink.util;

import com.mozilla.telemetry.ingestion.sink.config.SinkConfig;
import com.mozilla.telemetry.ingestion.sink.io.Input;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility for testing an unbounded {@link SinkConfig} with a termination condition.
 */
public class BoundedSink {

  /**
   * Run async until {@code messageCount} messages have been delivered.
   */
  private static CompletableFuture<Void> runAsync(int messageCount) throws IOException {
    final SinkConfig.Output output = SinkConfig.getOutput();
    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicReference<Input> input = new AtomicReference<>();
    input.set(SinkConfig.getInput(output.via(message -> output.apply(message).thenApplyAsync(v -> {
      final int currentMessages = counter.incrementAndGet();
      if (currentMessages >= messageCount) {
        input.get().stopAsync();
        StackdriverStatsExporter.unregister();
      }
      return v;
    }))));
    return CompletableFuture.runAsync(input.get()::run);
  }

  /**
   * Wait up to {@code timeout} seconds for {@code messageCount} messages to be delivered.
   */
  public static void run(int messageCount, int timeout) throws IOException {
    CompletableFuture<Void> future = runAsync(messageCount);
    CompletableFuture.anyOf(future, new TimedFuture(Duration.ofSeconds(timeout))).join();
    if (future.cancel(true)) {
      throw new CancellationException("Failed to deliver messages in " + timeout + " seconds");
    }
  }
}
