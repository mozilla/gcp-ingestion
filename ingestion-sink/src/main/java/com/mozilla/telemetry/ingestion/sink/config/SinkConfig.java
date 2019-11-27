package com.mozilla.telemetry.ingestion.sink.config;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
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

  public static final String OUTPUT_TABLE = "OUTPUT_TABLE";

  private static final String INPUT_SUBSCRIPTION = "INPUT_SUBSCRIPTION";
  private static final String BATCH_MAX_BYTES = "BATCH_MAX_BYTES";
  private static final String BATCH_MAX_DELAY = "BATCH_MAX_DELAY";
  private static final String BATCH_MAX_MESSAGES = "BATCH_MAX_MESSAGES";
  private static final String BIG_QUERY_OUTPUT_MODE = "BIG_QUERY_OUTPUT_MODE";
  private static final String LOAD_MAX_BYTES = "LOAD_MAX_BYTES";
  private static final String LOAD_MAX_DELAY = "LOAD_MAX_DELAY";
  private static final String LOAD_MAX_FILES = "LOAD_MAX_FILES";
  private static final String OUTPUT_BUCKET = "OUTPUT_BUCKET";
  private static final String OUTPUT_FORMAT = "OUTPUT_FORMAT";
  private static final String OUTPUT_TOPIC = "OUTPUT_TOPIC";
  private static final String OUTPUT_TOPIC_EXECUTOR_THREADS = "OUTPUT_TOPIC_EXECUTOR_THREADS";
  private static final String MAX_OUTSTANDING_ELEMENT_COUNT = "MAX_OUTSTANDING_ELEMENT_COUNT";
  private static final String MAX_OUTSTANDING_REQUEST_BYTES = "MAX_OUTSTANDING_REQUEST_BYTES";
  private static final String STREAMING_BATCH_MAX_BYTES = "STREAMING_BATCH_MAX_BYTES";
  private static final String STREAMING_BATCH_MAX_DELAY = "STREAMING_BATCH_MAX_DELAY";
  private static final String STREAMING_BATCH_MAX_MESSAGES = "STREAMING_BATCH_MAX_MESSAGES";
  private static final String STREAMING_MESSAGE_MAX_BYTES = "STREAMING_MESSAGE_MAX_BYTES";

  private static final List<String> INCLUDE_ENV_VARS = ImmutableList.of(INPUT_SUBSCRIPTION,
      BATCH_MAX_BYTES, BATCH_MAX_DELAY, BATCH_MAX_MESSAGES, OUTPUT_BUCKET, OUTPUT_FORMAT,
      BIG_QUERY_OUTPUT_MODE, LOAD_MAX_BYTES, LOAD_MAX_DELAY, LOAD_MAX_FILES, OUTPUT_TABLE,
      OUTPUT_TOPIC, OUTPUT_TOPIC_EXECUTOR_THREADS, MAX_OUTSTANDING_ELEMENT_COUNT,
      MAX_OUTSTANDING_REQUEST_BYTES, STREAMING_BATCH_MAX_BYTES, STREAMING_BATCH_MAX_DELAY,
      STREAMING_BATCH_MAX_MESSAGES, STREAMING_MESSAGE_MAX_BYTES);

  @VisibleForTesting
  protected static class Output implements Function<PubsubMessage, CompletableFuture<Void>> {

    private final Env env;
    private final OutputType type;
    private final Function<PubsubMessage, CompletableFuture<Void>> write;

    private Output(Env env, OutputType outputType,
        Function<PubsubMessage, CompletableFuture<Void>> write) {
      this.env = env;
      this.type = outputType;
      this.write = write;
    }

    public Output via(Function<PubsubMessage, CompletableFuture<Void>> write) {
      return new Output(this.env, this.type, write);
    }

    public CompletableFuture<Void> apply(PubsubMessage message) {
      return write.apply(message);
    }
  }

  enum OutputType {
    pubsub {

      @Override
      Output getOutput(Env env) {
        return new Output(env, this, new Pubsub.Write(env.getString(OUTPUT_TOPIC),
            env.getInt(OUTPUT_TOPIC_EXECUTOR_THREADS, 1), b -> b)::withoutResult);
      }
    },
    gcs {

      @Override
      Output getOutput(Env env) {
        return new Output(env, this, new Gcs.Write.Ndjson(getGcsService(env), //
            env.getLong(BATCH_MAX_BYTES, 100_000_000L), // default 100MB
            env.getInt(BATCH_MAX_MESSAGES, 1_000_000), // default 1M messages
            env.getDuration(BATCH_MAX_DELAY, "10m"), // default 10 minutes
            PubsubMessageToTemplatedString.of(getGcsOutputBucket(env)), getFormat(env),
            ignore -> CompletableFuture.completedFuture(null)));
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
    bigQueryLoad {

      @Override
      Output getOutput(Env env) {
        return new Output(env, this, new BigQuery.Load(getBigQueryService(env), getGcsService(env),
            // BigQuery Load API limits maximum bytes per request to 15TB
            env.getLong(LOAD_MAX_BYTES, 15_000_000_000_000L), // default 15TB
            // BigQuery Load API limits maximum files per request to 10,000
            env.getInt(LOAD_MAX_FILES, 10_000), // default 10K files
            // BigQuery Load API limits maximum load requests per table per day to 1,000 so with
            // three instances for redundancy that means each instance should load at most once
            // every 259.2 seconds or approximately 4.3 minutes
            env.getDuration(LOAD_MAX_DELAY, "10m"), // default 10 minutes
            BigQuery.Load.Delete.onSuccess)); // don't delete files until successfully loaded
      }
    },
    bigQueryFiles {

      @Override
      Output getOutput(Env env) {
        Output pubsubWrite = pubsub.getOutput(env);
        return new Output(env, this, new Gcs.Write.Ndjson(getGcsService(env),
            // BigQuery Load API limits maximum bytes per file to 4GB compressed or 5TB uncompressed
            env.getLong(BATCH_MAX_BYTES, 100_000_000L), // default 100MB
            // BigQuery Load API does not limit number of rows
            env.getInt(BATCH_MAX_MESSAGES, 1_000_000), // default 1M messages
            // Files will be loaded in batches so this is not directly impacted by BigQuery Load API
            // limits on maximum load requests
            env.getDuration(BATCH_MAX_DELAY, "1m"), // default 1 minute
            PubsubMessageToTemplatedString.forBigQuery(getBigQueryOutputBucket(env)),
            getFormat(env),
            // BigQuery Load API limits maximum load requests per table per day to 1,000 so send
            // blobInfo to pubsub and require loads be run separately to reduce maximum latency
            blobInfo -> pubsubWrite.apply(BlobInfoToPubsubMessage.apply(blobInfo))));
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
    bigQueryStreaming {

      @Override
      Output getOutput(Env env) {
        return new Output(env, this, new BigQuery.Write(getBigQueryService(env),
            // BigQuery.Write.Batch.getByteSize reports protobuf size, which can be ~1/3rd more
            // efficient than the JSON that actually gets sent over HTTP, so we use to 60% of the
            // 10MB API limit by default.
            env.getLong(BATCH_MAX_BYTES, 6_000_000L), // default 6MB
            // BigQuery Streaming API Limits maximum rows per request to 10,000
            env.getInt(BATCH_MAX_MESSAGES, 10_000), // default 10K messages
            env.getDuration(BATCH_MAX_DELAY, "1s"), // default 1 second
            PubsubMessageToTemplatedString.forBigQuery(env.getString(OUTPUT_TABLE)),
            getFormat(env)));
      }
    },
    bigQueryMixed {

      @Override
      Output getOutput(Env env) {
        final com.google.cloud.bigquery.BigQuery bigQuery = getBigQueryService(env);
        final Storage storage = getGcsService(env);
        Function<PubsubMessage, CompletableFuture<Void>> bigQueryLoad = new BigQuery.Load(bigQuery,
            storage,
            // BigQuery Load API limits maximum bytes per request to 15TB
            env.getLong(LOAD_MAX_BYTES, 15_000_000_000_000L), // default 15TB
            // BigQuery Load API limits maximum files per request to 10,000
            env.getInt(LOAD_MAX_FILES, 10_000), // default 10K files
            // BigQuery Load API limits maximum loads per table per day to 1,000 but mixed mode
            // expects fewer than 1,000 messages per day to need file loads
            env.getDuration(LOAD_MAX_DELAY, "1s"), // default 1s
            BigQuery.Load.Delete.always); // files will be recreated if not successfully loaded
        // Combine bigQueryFiles and bigQueryLoad without an intermediate PubSub topic
        Function<PubsubMessage, CompletableFuture<Void>> fileOutput = new Gcs.Write.Ndjson(storage,
            env.getLong(BATCH_MAX_BYTES, 100_000_000L), // default 100MB
            env.getInt(BATCH_MAX_MESSAGES, 1_000_000), // default 1M messages
            env.getDuration(BATCH_MAX_DELAY, "10m"), // default 10 minutes
            PubsubMessageToTemplatedString.forBigQuery(getBigQueryOutputBucket(env)),
            getFormat(env),
            blobInfo -> bigQueryLoad.apply(BlobInfoToPubsubMessage.apply(blobInfo)));
        // Like bigQueryStreaming, but use STREAMING_ prefix for batch configuration
        Function<PubsubMessage, CompletableFuture<Void>> streamingOutput = new BigQuery.Write(
            bigQuery,
            // BigQuery.Write.Batch.getByteSize reports protobuf size, which can be ~1/3rd more
            // efficient than the JSON that actually gets sent over HTTP, so we use to 60% of the
            // 10MB API limit by default.
            env.getLong(STREAMING_BATCH_MAX_BYTES, 6_000_000L), // default 6MB
            // BigQuery Streaming API Limits maximum rows per request to 10,000
            env.getInt(STREAMING_BATCH_MAX_MESSAGES, 10_000), // default 10K messages
            env.getDuration(STREAMING_BATCH_MAX_DELAY, "1s"), // default 1 second
            PubsubMessageToTemplatedString.forBigQuery(env.getString(OUTPUT_TABLE)),
            getFormat(env));
        // BigQuery Streaming API Limits maximum row size to 1MiB
        long maxStreamingSize = env.getLong(STREAMING_MESSAGE_MAX_BYTES, 1_000_000L); // default 1MB
        return new Output(env, this, message -> {
          if (message.getData().size() > maxStreamingSize) {
            return fileOutput.apply(message);
          } else {
            return streamingOutput.apply(message);
          }
        });
      }
    };

    // Each case in the enum must implement this method to define how to write out messages.
    abstract Output getOutput(Env env);

    // Cases in the enum may override this method set a more appropriate default.
    long getDefaultMaxOutstandingElementCount() {
      return 50_000L; // 50K messages
    }

    // Cases in the enum may override this method set a more appropriate default.
    long getDefaultMaxOutstandingRequestBytes() {
      return 100_000_000L; // 100MB
    }

    static OutputType get(Env env) {
      boolean hasBigQueryOutputMode = env.containsKey(BIG_QUERY_OUTPUT_MODE);
      if (env.containsKey(OUTPUT_BUCKET) && !hasBigQueryOutputMode) {
        return OutputType.gcs;
      } else if (env.containsKey(OUTPUT_TOPIC) && !hasBigQueryOutputMode) {
        return OutputType.pubsub;
      } else if (env.containsKey(OUTPUT_TABLE) || hasBigQueryOutputMode) {
        final String outputMode = env.getString(BIG_QUERY_OUTPUT_MODE, "streaming").toLowerCase();
        if (outputMode.equals("streaming")) {
          return OutputType.bigQueryStreaming;
        }
        if (outputMode.equals("mixed")) {
          return OutputType.bigQueryMixed;
        }
        if (outputMode.equals("file_loads")) {
          return OutputType.bigQueryFiles;
        }
        throw new IllegalArgumentException("Unsupported BIG_QUERY_OUTPUT_MODE: " + outputMode);
      } else {
        return OutputType.bigQueryLoad;
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

  private static String getGcsOutputBucket(Env env) {
    final String outputBucket = env.getString(OUTPUT_BUCKET);
    if (outputBucket.endsWith("/")) {
      return outputBucket;
    }
    // Append / to OUTPUT_BUCKET to enforce that it will be a directory
    return outputBucket + "/";
  }

  private static String getBigQueryOutputBucket(Env env) {
    // Append OUTPUT_TABLE to ensure separate files per table
    return getGcsOutputBucket(env) + OUTPUT_TABLE + "=" + env.getString(OUTPUT_TABLE) + "/";
  }

  private static com.google.cloud.bigquery.BigQuery getBigQueryService(Env env) {
    return BigQueryOptions.getDefaultInstance().getService();
  }

  private static Storage getGcsService(Env env) {
    return StorageOptions.getDefaultInstance().getService();
  }

  /** Return a configured output transform. */
  public static Output getOutput() {
    Env env = new Env(INCLUDE_ENV_VARS);
    return OutputType.get(env).getOutput(env);
  }

  /** Return a configured input transform. */
  public static Pubsub.Read getInput(Output output) {
    // read pubsub messages from INPUT_SUBSCRIPTION
    Pubsub.Read input = new Pubsub.Read(output.env.getString(INPUT_SUBSCRIPTION), output,
        builder -> builder.setFlowControlSettings(FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(output.type.getMaxOutstandingElementCount(output.env))
            .setMaxOutstandingRequestBytes(output.type.getMaxOutstandingRequestBytes(output.env))
            .build()));
    output.env.requireAllVarsUsed();
    return input;
  }
}
