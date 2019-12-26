package com.mozilla.telemetry.ingestion.sink.config;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.io.BigQuery;
import com.mozilla.telemetry.ingestion.sink.io.Gcs;
import com.mozilla.telemetry.ingestion.sink.io.Pubsub;
import com.mozilla.telemetry.ingestion.sink.transform.BlobInfoToPubsubMessage;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import com.mozilla.telemetry.ingestion.sink.util.Env;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class SinkConfig {

  private static final String INPUT_SUBSCRIPTION = "INPUT_SUBSCRIPTION";
  private static final String BATCH_MAX_BYTES = "BATCH_MAX_BYTES";
  private static final String BATCH_MAX_MESSAGES = "BATCH_MAX_MESSAGES";
  private static final String BATCH_MAX_DELAY = "BATCH_MAX_DELAY";
  private static final String OUTPUT_FORMAT = "OUTPUT_FORMAT";
  private static final String OUTPUT_BUCKET = "OUTPUT_BUCKET";
  private static final String OUTPUT_TABLE = "OUTPUT_TABLE";
  private static final String OUTPUT_TOPIC = "OUTPUT_TOPIC";
  private static final String OUTPUT_TOPIC_EXECUTOR_THREADS = "OUTPUT_TOPIC_EXECUTOR_THREADS";
  private static final String MAX_OUTSTANDING_ELEMENT_COUNT = "MAX_OUTSTANDING_ELEMENT_COUNT";
  private static final String MAX_OUTSTANDING_REQUEST_BYTES = "MAX_OUTSTANDING_REQUEST_BYTES";
  private static final List<String> INCLUDE_ENV_VARS = ImmutableList.of(INPUT_SUBSCRIPTION,
      BATCH_MAX_BYTES, BATCH_MAX_MESSAGES, BATCH_MAX_DELAY, OUTPUT_FORMAT, OUTPUT_BUCKET,
      OUTPUT_TABLE, OUTPUT_TOPIC, OUTPUT_TOPIC_EXECUTOR_THREADS, MAX_OUTSTANDING_ELEMENT_COUNT,
      MAX_OUTSTANDING_REQUEST_BYTES);

  enum OutputType {
    pubsub {

      @Override
      Function<PubsubMessage, CompletableFuture<Void>> getOutput(Env env) {
        return new Pubsub.Write(env.getString(OUTPUT_TOPIC),
            env.getInt(OUTPUT_TOPIC_EXECUTOR_THREADS, 1), b -> b)::withoutResult;
      }
    },
    gcs {

      @Override
      Function<PubsubMessage, CompletableFuture<Void>> getOutput(Env env) {
        String gcsPrefix = env.getString(OUTPUT_BUCKET);
        // Append / to GCS prefix to enforce that it will be a directory
        if (!gcsPrefix.endsWith("/")) {
          gcsPrefix += "/";
        }
        // Append OUTPUT_TABLE to GCS prefix if present
        if (env.containsKey(OUTPUT_TABLE)) {
          gcsPrefix += "output_table=" + env.getString(OUTPUT_TABLE) + "/";
        }

        // if OUTPUT_TOPIC is present send notification for each gcs blob written
        final Function<BlobInfo, CompletableFuture<Void>> batchCloseHook;
        if (env.containsKey(OUTPUT_TOPIC)) {
          Function<PubsubMessage, CompletableFuture<Void>> pubsubWrite = pubsub.getOutput(env);
          batchCloseHook = blobInfo -> pubsubWrite.apply(BlobInfoToPubsubMessage.apply(blobInfo));
        } else {
          batchCloseHook = ignore -> CompletableFuture.completedFuture(null);
        }

        return new Gcs.Write.Ndjson(StorageOptions.getDefaultInstance().getService(),
            env.getLong(BATCH_MAX_BYTES, 100_000_000L), // default 100MB
            env.getInt(BATCH_MAX_MESSAGES, 1_000_000), // default 1M messages
            env.getDuration(BATCH_MAX_DELAY, "10m"), // default 10 minutes
            PubsubMessageToTemplatedString.of(gcsPrefix), getFormat(env), batchCloseHook);
      }

      @Override
      long getDefaultMaxOutstandingElementCount() {
        return 1_000_000L; // 1M messages
      }

      @Override
      long getDefaultMaxOutstandingRequestBytes() {
        return 1_000_000_000L; // 1GB
      }
    },
    bigQuery {

      @Override
      Function<PubsubMessage, CompletableFuture<Void>> getOutput(Env env) {
        return new BigQuery.Write(BigQueryOptions.getDefaultInstance().getService(),
            // BigQuery.Write.Batch.getByteSize reports protobuf size, which can be ~1/3rd more
            // efficient than the JSON that actually gets sent over HTTP, so we use to 60% of the
            // 10MB API limit by default.
            env.getLong(BATCH_MAX_BYTES, 6_000_000L), // default 6MB
            // BigQuery Streaming API Limits maximum rows per request to 10,000
            env.getInt(BATCH_MAX_MESSAGES, 10_000), // default 10K messages
            env.getDuration(BATCH_MAX_DELAY, "1s"), // default 1 second
            PubsubMessageToTemplatedString.forBigQuery(env.getString(OUTPUT_TABLE)),
            getFormat(env));
      }
    };

    // Each case in the enum must implement this method to define how to write out messages.
    abstract Function<PubsubMessage, CompletableFuture<Void>> getOutput(Env env);

    // Cases in the enum may override this method set a more appropriate default.
    long getDefaultMaxOutstandingElementCount() {
      return 50_000L; // 50K messages
    }

    // Cases in the enum may override this method set a more appropriate default.
    long getDefaultMaxOutstandingRequestBytes() {
      return 100_000_000L; // 100MB
    }

    static OutputType get(Env env) {
      if (env.containsKey(OUTPUT_BUCKET)) {
        return OutputType.gcs;
      } else if (env.containsKey(OUTPUT_TOPIC)) {
        return OutputType.pubsub;
      } else if (env.containsKey(OUTPUT_TABLE)) {
        return OutputType.bigQuery;
      } else {
        throw new IllegalArgumentException("Could not find at least one of [OUTPUT_BUCKET,"
            + " OUTPUT_TOPIC, OUTPUT_TABLE] in environment");
      }
    }

    final long getMaxOutstandingElementCount(Env env) {
      return env.getLong(MAX_OUTSTANDING_ELEMENT_COUNT, getDefaultMaxOutstandingElementCount());
    }

    final long getMaxOutstandingRequestBytes(Env env) {
      return env.getLong(MAX_OUTSTANDING_REQUEST_BYTES, getDefaultMaxOutstandingRequestBytes());
    }
  }

  private static PubsubMessageToObjectNode.Format getFormat(Env env) {
    return PubsubMessageToObjectNode.Format.valueOf(env.getString(OUTPUT_FORMAT, "raw"));
  }

  public static Pubsub.Read getInput() {
    Env env = new Env(INCLUDE_ENV_VARS);
    OutputType outputType = OutputType.get(env);
    // read pubsub messages from INPUT_SUBSCRIPTION
    Pubsub.Read input = new Pubsub.Read(env.getString(INPUT_SUBSCRIPTION),
        outputType.getOutput(env),
        builder -> builder.setFlowControlSettings(FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(outputType.getMaxOutstandingElementCount(env))
            .setMaxOutstandingRequestBytes(outputType.getMaxOutstandingRequestBytes(env)).build()));
    env.requireAllVarsUsed();
    return input;
  }
}
