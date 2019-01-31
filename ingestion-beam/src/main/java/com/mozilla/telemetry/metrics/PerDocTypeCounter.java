/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

/** Convenience methods for managing sets of counters separated by document_type. */
public class PerDocTypeCounter {

  private static final Map<String, String> EMPTY_ATTRIBUTES = ImmutableMap.of();
  private static final Map<String, Counter> counters = new HashMap<>();

  /**
   * Like {@link #inc(Map, String, long)}, but always increments by 1.
   */
  public static void inc(@Nullable Map<String, String> attributes, String name) {
    inc(attributes, name, 1);
  }

  /**
   * Increment a counter segmented by docType.
   *
   * <p>Metrics will be named like {@code
   * ${document_namespace}/${document_type}_v${document_version}/name} based on the passed map of
   * attributes and the passed metric name. Because we want to keep counters at various points of
   * message parsing, not all of those attributes may be available. If version is missing, the _v
   * suffix will be skipped. A null attribute map or missing namespace and docType will be replaced
   * with unknown_namespace/unknown_doctype.
   *
   * <p>Stackdriver allows component "paths" separated by slashes. The Stackdriver UI will show the
   * name of the metric as the entire user-defined path while the Dataflow UI will the final path
   * component as the "Counter name", appending the rest of the path to the value it shows as
   * "Step".
   *
   * <p>We change dashes to underscores to match Stackdriver naming restrictions. See
   * https://cloud.google.com/monitoring/api/v3/metrics-details#label_names
   *
   * @param attributes a map of attributes from a PubsubMessage
   * @param name name to be used as the final component of the path
   * @param n the number by which to increment the counter
   */
  public static void inc(@Nullable Map<String, String> attributes, String name, long n) {
    getOrCreateCounter(attributes, name).inc(n);
  }

  @VisibleForTesting
  static Counter getOrCreateCounter(@Nullable Map<String, String> attributes, String name) {
    if (attributes == null) {
      attributes = EMPTY_ATTRIBUTES;
    }
    String namespace = attributes.getOrDefault("document_namespace", "unknown_namespace");
    // Dataflow's UI collapses the metric name, but always shows the penultimate path component
    // as part of "Step", so we're a bit more verbose with the default doctype value.
    String docType = attributes.getOrDefault("document_type", "unknown_doctype");
    String version = attributes.get("document_version");
    if (version != null) {
      docType = docType + "_v" + version;
    }
    String key = String.format("%s/%s/%s", namespace, docType, name)
        // We change dashes to underscores to make sure we're following Stackdriver's naming scheme:
        // https://cloud.google.com/monitoring/api/v3/metrics-details#label_names
        .replace("-", "_");
    return counters.computeIfAbsent(key, k -> Metrics.counter(PerDocTypeCounter.class, k));
  }
}
