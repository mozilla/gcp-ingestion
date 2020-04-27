package com.mozilla.telemetry.ingestion.sink.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Aggregation.Distribution;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.ViewManager;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public abstract class BatchWrite<InputT, EncodedT, BatchKeyT, BatchResultT>
    implements Function<InputT, CompletableFuture<Void>> {

  /**
   * Create exponential distribution for aggregating values.
   *
   * <p>Round boundaries to the nearest integer and remove duplicates.
   */
  private static Distribution exponentialDistribution(double base, double expStart, double expStep,
      long steps) {
    return Distribution
        .create(BucketBoundaries.create(DoubleStream.iterate(expStart, v -> v + expStep)
            .limit(steps + 1).map(exp -> Math.round(Math.pow(base, exp))).distinct().boxed()
            .collect(Collectors.toList())));
  }

  private static final StatsRecorder STATS_RECORDER = Stats.getStatsRecorder();
  // distribution from 1 to 1,000,000
  private static final Distribution BATCH_MESSAGES_AGG = exponentialDistribution(
      // 10 messages
      10,
      // 10 ^ 0 == 1 message
      0,
      // multiply by 10 every 5 steps
      1. / 5,
      // limit exponent to 6; 10 ^ 6 messages == 1,000,000 messages
      6 * 5);
  // distribution from 128 bytes to 1 GiB
  private static final Distribution BATCH_BYTES_AGG = exponentialDistribution(
      // 2 bytes
      2,
      // 2 ^ 7 == 128 bytes
      7,
      // multiply by 2 every step
      1,
      // limit exponent to 30; 2 ^ 30 bytes == 1GiB
      30 - 7);
  // distribution 0 to 1 hour in milliseconds
  private static final Distribution BATCH_DELAY_AGG = exponentialDistribution(
      // 1 minute
      60000,
      // 60,000 ^ -1 == 0 ms
      -1,
      // multiply by 60,000 every 8 steps
      1. / 8,
      // limit exponent to 2; 60,000 ^ 2 ms == 1hr
      (2 - (-1)) * 8);
  private static final Aggregation.Sum SUM_AGG = Aggregation.Sum.create();
  private static final Aggregation.Count COUNT_AGG = Aggregation.Count.create();

  private final MeasureLong batchCount;
  private final MeasureLong batchBytes;
  private final MeasureLong batchMessages;
  private final MeasureLong batchDelay;
  private final MeasureLong totalBytes;
  private final MeasureLong totalMessages;

  protected final PubsubMessageToTemplatedString batchKeyTemplate;
  protected final Executor executor;

  private final long maxBytes;
  private final int maxMessages;
  private final Duration maxDelay;

  /** Constructor. */
  public BatchWrite(long maxBytes, int maxMessages, Duration maxDelay,
      PubsubMessageToTemplatedString batchKeyTemplate, Executor executor) {
    this.maxBytes = maxBytes;
    this.maxMessages = maxMessages;
    this.maxDelay = maxDelay;
    this.batchKeyTemplate = batchKeyTemplate;
    this.executor = executor;

    // create OpenCensus measures with a class name prefix
    final String shortClassName = this.getClass().getName().replaceAll(".*[.]", "");
    final String batchType = shortClassName.replace('$', '_').toLowerCase();
    totalBytes = MeasureLong.create(batchType + "_total_bytes",
        "The number of bytes received in " + shortClassName, "B");
    totalMessages = MeasureLong.create(batchType + "_total_messages",
        "The number of messages received in " + shortClassName, "1");
    batchCount = MeasureLong.create(batchType + "_batch_count",
        "The number of batches closed in " + shortClassName, "1");
    batchBytes = MeasureLong.create(batchType + "_batch_bytes",
        "Distribution of the number of bytes in a batch in " + shortClassName, "B");
    batchMessages = MeasureLong.create(batchType + "_batch_messages",
        "Distribution of the number of messages in a batch in " + shortClassName, "1");
    batchDelay = MeasureLong.create(batchType + "_batch_delay",
        "Distribution of the number of milliseconds a batch waited for messages in "
            + shortClassName,
        "ms");
  }

  /**
   * Register a view for every measure.
   *
   * <p>If this is not called, e.g. during unit tests, recorded values will not be exported.
   */
  public Function<InputT, CompletableFuture<Void>> withOpenCensusMetrics() {
    final ViewManager viewManager = Stats.getViewManager();
    ImmutableMap.<MeasureLong, Aggregation>builder().put(batchCount, COUNT_AGG)
        .put(batchBytes, BATCH_BYTES_AGG).put(batchMessages, BATCH_MESSAGES_AGG)
        .put(batchDelay, BATCH_DELAY_AGG).put(totalBytes, SUM_AGG).put(totalMessages, SUM_AGG)
        .build()
        .forEach((measure, aggregation) -> viewManager
            .registerView(View.create(View.Name.create(measure.getName()), measure.getDescription(),
                measure, aggregation, ImmutableList.of())));
    return this;
  }

  /** Record metrics for a Batch. */
  private void recordBatchMetrics(long bytes, int messages, long delayMillis) {
    STATS_RECORDER.newMeasureMap().put(batchCount, 1).put(batchBytes, bytes)
        .put(batchMessages, messages).put(batchDelay, delayMillis).put(totalBytes, bytes)
        .put(totalMessages, messages).record();
  }

  @VisibleForTesting
  public final ConcurrentMap<BatchKeyT, Batch> batches = new ConcurrentHashMap<>();

  @Override
  public CompletableFuture<Void> apply(InputT input) {
    AtomicReference<CompletableFuture<Void>> output = new AtomicReference<>();
    EncodedT encodedInput = encodeInput(input);
    batches.compute(getBatchKey(input), (batchKey, batch) -> {
      if (batch != null) {
        batch.add(encodedInput).ifPresent(output::set);
      }
      if (output.get() == null) {
        batch = getBatch(batchKey);
        // add first input before allowing batch
        batch.add(encodedInput).ifPresent(output::set);
        // allow batch to complete by timeout now that we have attempted to add the first item
        batch.init.complete(null);
      }
      return batch;
    });

    return Optional.ofNullable(output.get())
        // this is only reachable if getBatch returns a closed batch,
        .orElseThrow(() -> new IllegalArgumentException("Empty batch rejected input"));
  }

  public abstract class Batch {

    // block this batch from completing by timeout until this future is completed
    final CompletableFuture<Void> init = new CompletableFuture<>();

    // wait for init then mark this batch full after maxDelay
    @VisibleForTesting
    final CompletableFuture<Void> full = init.thenComposeAsync(v -> new TimedFuture(maxDelay));

    // wait for full then synchronize and close
    private final CompletableFuture<BatchResultT> result = full
        .thenComposeAsync(this::synchronousClose, executor);

    @VisibleForTesting
    public int size = 0;

    @VisibleForTesting
    public long byteSize = 0;

    private final long startNanos = System.nanoTime();

    /**
     * Call close from a synchronized context.
     */
    private synchronized CompletableFuture<BatchResultT> synchronousClose(Void ignore) {
      recordBatchMetrics(byteSize, size, (System.nanoTime() - startNanos) / 1_000_000);
      return close();
    }

    private synchronized Optional<CompletableFuture<Void>> add(EncodedT encodedInput) {
      if (full.isDone()) {
        return Optional.empty();
      }
      int newSize = size + 1;
      long newByteSize = byteSize + getByteSize(encodedInput);
      if (size > 0 && (newSize > maxMessages || newByteSize > maxBytes)) {
        this.full.complete(null);
        return Optional.empty();
      }
      size = newSize;
      byteSize = newByteSize;
      write(encodedInput);
      // Synchronous CompletableFuture methods are executed by the thread that completes the
      // future, or the current thread if the future is already complete. Use that here to
      // minimize memory usage by resolving the returned future as soon as possible.
      return Optional.of(result.thenAccept(result -> this.checkResultFor(result, newSize - 1)));
    }

    protected void checkResultFor(BatchResultT batchResult, int index) {
    }

    protected abstract CompletableFuture<BatchResultT> close();

    protected abstract void write(EncodedT encodedInput);

    protected abstract long getByteSize(EncodedT encodedInput);
  }

  protected abstract BatchKeyT getBatchKey(InputT input);

  protected abstract EncodedT encodeInput(InputT input);

  protected abstract Batch getBatch(BatchKeyT batchKey);
}
