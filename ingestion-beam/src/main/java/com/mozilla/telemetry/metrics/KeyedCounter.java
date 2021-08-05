package com.mozilla.telemetry.metrics;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

/**
 * Collection of counters separated by arbitrary keys.
 */
public class KeyedCounter {

  static final Map<String, Counter> counters = new ConcurrentHashMap<>();

  /**
   * Increment the counter associated with the given key by 1.
   */
  public static void inc(String key) {
    inc(key, 1);
  }

  /**
   * Increment the counter associated with the given key by n.
   */
  public static void inc(String key, long n) {
    getOrCreateCounter(key).inc(n);
  }

  @VisibleForTesting
  static Counter getOrCreateCounter(String key) {
    return counters.computeIfAbsent(key, k -> Metrics.counter(KeyedCounter.class, k));
  }
}
