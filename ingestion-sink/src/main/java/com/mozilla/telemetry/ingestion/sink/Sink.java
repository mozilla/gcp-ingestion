/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.io.BigQuery;
import com.mozilla.telemetry.ingestion.sink.io.Gcs;
import com.mozilla.telemetry.ingestion.sink.io.Pubsub;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToJSONObject.Format;
import com.mozilla.telemetry.ingestion.sink.util.Env;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Sink {

  private Sink() {
  }

  public static void main(String[] args) {
    main().run(); // run pubsub reader
  }

  private static final String BATCH_MAX_BYTES = "BATCH_MAX_BYTES";
  private static final String BATCH_MAX_MESSAGES = "BATCH_MAX_MESSAGES";
  private static final String BATCH_MAX_DELAY = "BATCH_MAX_DELAY";
  private static final String INPUT_SUBSCRIPTION = "INPUT_SUBSCRIPTION";
  private static final String OUTPUT_FORMAT = "OUTPUT_FORMAT";
  private static final String OUTPUT_BUCKET = "OUTPUT_BUCKET";
  private static final String OUTPUT_TABLE = "OUTPUT_TABLE";
  private static final String OUTPUT_TOPIC = "OUTPUT_TOPIC";
  private static final String OUTPUT_TOPIC_EXECUTOR_THREADS = "OUTPUT_TOPIC_EXECUTOR_THREADS";
  private static final String MAX_OUTSTANDING_ELEMENT_COUNT = "MAX_OUTSTANDING_ELEMENT_COUNT";
  private static final String MAX_OUTSTANDING_REQUEST_BYTES = "MAX_OUTSTANDING_REQUEST_BYTES";

  @VisibleForTesting
  static Pubsub.Read main() {
    final Format format = Format.valueOf(Env.getString(OUTPUT_FORMAT, "raw"));

    final Function<PubsubMessage, CompletableFuture<Void>> output;
    final long maxOutstandingElementCount;
    final long maxOutstandingRequestBytes;
    if (Env.optString(OUTPUT_BUCKET).isPresent()) {
      String gcsPrefix = Env.getString(OUTPUT_BUCKET);
      // Append - to output prefix if necessary to separate each blob's UUID from other parts
      if (gcsPrefix.contains("/") && !gcsPrefix.endsWith("/")) {
        gcsPrefix = gcsPrefix + "-";
      }
      output = new Gcs.Write.Ndjson(StorageOptions.getDefaultInstance().getService(),
          Env.getLong(BATCH_MAX_BYTES, 100_000_000L), // default 100MB
          Env.getInt(BATCH_MAX_MESSAGES, 1_000_000), // default 1M messages
          Env.getDuration(BATCH_MAX_DELAY, "10m"), // default 10 minutes
          gcsPrefix, format);
      // These limits are higher for the GCS output because it has higher batch latency
      // default 1M messages
      maxOutstandingElementCount = Env.getLong(MAX_OUTSTANDING_ELEMENT_COUNT, 1_000_000L);
      // default 1GB
      maxOutstandingRequestBytes = Env.getLong(MAX_OUTSTANDING_REQUEST_BYTES, 1_000_000_000L);
    } else {
      if (Env.optString(OUTPUT_TOPIC).isPresent()) {
        output = new Pubsub.Write(Env.getString(OUTPUT_TOPIC),
            Env.getInt(OUTPUT_TOPIC_EXECUTOR_THREADS, 1), b -> b)::withoutResult;
      } else {
        output = new BigQuery.Write(BigQueryOptions.getDefaultInstance().getService(),
            // BigQuery.Write.Batch.getByteSize reports protobuf size, which can be ~1/3rd more
            // efficient than the JSON that actually gets sent over HTTP, so we use to 60% of the
            // 10MB API limit by default.
            Env.getLong(BATCH_MAX_BYTES, 6_000_000L), // default 6MB
            // BigQuery Streaming API Limits maximum rows per request to 10,000
            Env.getInt(BATCH_MAX_MESSAGES, 10_000), // default 10K messages
            Env.getDuration(BATCH_MAX_DELAY, "1s"), // default 1 second
            Env.getString(OUTPUT_TABLE), format);
      }
      // default 50K messages
      maxOutstandingElementCount = Env.getLong(MAX_OUTSTANDING_ELEMENT_COUNT, 50_000L);
      // default 100MB
      maxOutstandingRequestBytes = Env.getLong(MAX_OUTSTANDING_REQUEST_BYTES, 100_000_000L);
    }

    // read pubsub messages from INPUT_SUBSCRIPTION
    return new Pubsub.Read(Env.getString(INPUT_SUBSCRIPTION), output,
        builder -> builder.setFlowControlSettings(FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(maxOutstandingElementCount)
            .setMaxOutstandingRequestBytes(maxOutstandingRequestBytes).build()));
  }
}
