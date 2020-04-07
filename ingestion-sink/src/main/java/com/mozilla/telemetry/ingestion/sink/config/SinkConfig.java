package com.mozilla.telemetry.ingestion.sink.config;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.io.Amplitude;
import com.mozilla.telemetry.ingestion.sink.io.BigQuery;
import com.mozilla.telemetry.ingestion.sink.io.BigQuery.BigQueryErrors;
import com.mozilla.telemetry.ingestion.sink.io.Gcs;
import com.mozilla.telemetry.ingestion.sink.io.Pubsub;
import com.mozilla.telemetry.ingestion.sink.transform.BlobIdToPubsubMessage;
import com.mozilla.telemetry.ingestion.sink.transform.CompressPayload;
import com.mozilla.telemetry.ingestion.sink.transform.DecompressPayload;
import com.mozilla.telemetry.ingestion.sink.transform.DocumentTypePredicate;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import com.mozilla.telemetry.ingestion.sink.util.Env;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Predicate;

public class SinkConfig {

  public static final String OUTPUT_TABLE = "OUTPUT_TABLE";

  private static final String AMPLITUDE_API_KEY = "AMPLITUDE_API_KEY";
  private static final String AMPLITUDE_SECRET_KEY = "AMPLITUDE_SECRET_KEY";
  private static final String INPUT_COMPRESSION = "INPUT_COMPRESSION";
  private static final String INPUT_PARALLELISM = "INPUT_PARALLELISM";
  private static final String INPUT_SUBSCRIPTION = "INPUT_SUBSCRIPTION";
  private static final String BATCH_MAX_BYTES = "BATCH_MAX_BYTES";
  private static final String BATCH_MAX_DELAY = "BATCH_MAX_DELAY";
  private static final String BATCH_MAX_MESSAGES = "BATCH_MAX_MESSAGES";
  private static final String BIG_QUERY_OUTPUT_MODE = "BIG_QUERY_OUTPUT_MODE";
  private static final String LOAD_MAX_BYTES = "LOAD_MAX_BYTES";
  private static final String LOAD_MAX_DELAY = "LOAD_MAX_DELAY";
  private static final String LOAD_MAX_FILES = "LOAD_MAX_FILES";
  private static final String OUTPUT_BUCKET = "OUTPUT_BUCKET";
  private static final String OUTPUT_COMPRESSION = "OUTPUT_COMPRESSION";
  private static final String OUTPUT_FORMAT = "OUTPUT_FORMAT";
  private static final String OUTPUT_PARALLELISM = "OUTPUT_PARALLELISM";
  private static final String OUTPUT_TOPIC = "OUTPUT_TOPIC";
  private static final String MAX_OUTSTANDING_ELEMENT_COUNT = "MAX_OUTSTANDING_ELEMENT_COUNT";
  private static final String MAX_OUTSTANDING_REQUEST_BYTES = "MAX_OUTSTANDING_REQUEST_BYTES";
  private static final String SCHEMAS_LOCATION = "SCHEMAS_LOCATION";
  private static final String STREAMING_BATCH_MAX_BYTES = "STREAMING_BATCH_MAX_BYTES";
  private static final String STREAMING_BATCH_MAX_DELAY = "STREAMING_BATCH_MAX_DELAY";
  private static final String STREAMING_BATCH_MAX_MESSAGES = "STREAMING_BATCH_MAX_MESSAGES";
  private static final String STREAMING_DOCTYPES = "STREAMING_DOCTYPES";
  private static final String STRICT_SCHEMA_DOCTYPES = "STRICT_SCHEMA_DOCTYPES";

  private static final Set<String> INCLUDE_ENV_VARS = ImmutableSet.of(AMPLITUDE_API_KEY,
      AMPLITUDE_SECRET_KEY, INPUT_COMPRESSION, INPUT_PARALLELISM, INPUT_SUBSCRIPTION,
      BATCH_MAX_BYTES, BATCH_MAX_DELAY, BATCH_MAX_MESSAGES, BIG_QUERY_OUTPUT_MODE, LOAD_MAX_BYTES,
      LOAD_MAX_DELAY, LOAD_MAX_FILES, OUTPUT_BUCKET, OUTPUT_COMPRESSION, OUTPUT_FORMAT,
      OUTPUT_PARALLELISM, OUTPUT_TABLE, OUTPUT_TOPIC, MAX_OUTSTANDING_ELEMENT_COUNT,
      MAX_OUTSTANDING_REQUEST_BYTES, SCHEMAS_LOCATION, STREAMING_BATCH_MAX_BYTES,
      STREAMING_BATCH_MAX_DELAY, STREAMING_BATCH_MAX_MESSAGES, STREAMING_DOCTYPES,
      STRICT_SCHEMA_DOCTYPES);

  // BigQuery.Write.Batch.getByteSize reports protobuf size, which can be ~1/3rd more
  // efficient than the JSON that actually gets sent over HTTP, so we use to 60% of the
  // 10MB API limit by default.
  private static final long DEFAULT_STREAMING_BATCH_MAX_BYTES = 6_000_000L; // 6MB
  // BigQuery Streaming API Limits maximum rows per request to 10,000.
  private static final int DEFAULT_STREAMING_BATCH_MAX_MESSAGES = 10_000; // 10,000
  // Messages delivered via streaming are expected to have a total pipeline delay (including edge
  // and decoder) of less than 1 minute, and ideally less than 10 seconds.
  private static final String DEFAULT_STREAMING_BATCH_MAX_DELAY = "1s"; // 1 second
  // BigQuery Load API limits maximum bytes per file to 5TB uncompressed. The api also
  // limits maximum files per request to 10,000 and the default for LOAD_MAX_BYTES is
  // 100GB, so at least 10MB per file is needed for bigQueryLoad to maximize load request
  // size. The default is intended to limit memory usage and how much data is
  // reprocessed due to errors, while staying above 10MB to maximize load request size.
  private static final long DEFAULT_BATCH_MAX_BYTES = 100_000_000L; // 100MB
  // BigQuery Load API does not limit number of rows, but rows are generally over 100 bytes in size,
  // so setting this any higher than BATCH_MAX_BYTES/100 is expected to have no impact.
  private static final int DEFAULT_BATCH_MAX_MESSAGES = 1_000_000; // 1,000,000
  // BigQuery Load API limits maximum files per request to 10,000, and at least 3 load requests are
  // expected every DEFAULT_FILE_LOAD_MAX_DELAY, so 3 requests every 9 minutes and 300 sinks writing
  // files this should be at minimum 5.4 seconds. Messages not delivered via streaming are expected
  // to have a total pipeline delay (including edge and decoder) of less than 1 hour, and ideally
  // less than 10 minutes, so this plus DEFAULT_LOAD_MAX_DELAY should be less than 10 minutes.
  // Messages may be kept in memory until they ack or nack, so too much delay can cause OOM errors.
  private static final String DEFAULT_BATCH_MAX_DELAY = "1m"; // 1 minute
  // BigQuery Load API limits maximum bytes per request to 15TB, but load requests for clustered
  // tables fail when sorting that much data, so to avoid that issue the default is lower.
  private static final long DEFAULT_LOAD_MAX_BYTES = 100_000_000_000L; // 100GB
  // BigQuery Load API limits maximum files per request to 10,000.
  private static final int DEFAULT_LOAD_MAX_FILES = 10_000; // 10,000
  // BigQuery Load API limits maximum load requests per table per day to 1,000, so with
  // 3 instances for redundancy that means each instance should load at most once
  // every 259.2 seconds or approximately 4.3 minutes. Messages not delivered via streaming are
  // expected to have a total pipeline delay (including edge and decoder) of less than 1 hour, and
  // ideally less than 10 minutes, so this plus DEFAULT_BATCH_MAX_DELAY should be about 10 minutes.
  private static final String DEFAULT_LOAD_MAX_DELAY = "9m"; // 9 minutes
  // BigQuery Load API limits maximum load requests per table per day to 1,000 but mixed mode
  // expects fewer than 1,000 messages per day to need file loads, so use streaming max delay.
  private static final String DEFAULT_STREAMING_LOAD_MAX_DELAY = DEFAULT_STREAMING_BATCH_MAX_DELAY;

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

      private CompressPayload getOutputCompression(Env env) {
        return CompressPayload.valueOf(
            env.getString(OUTPUT_COMPRESSION, CompressPayload.NONE.toString()).toUpperCase());
      }

      @Override
      Output getOutput(Env env, Executor executor) {
        return new Output(env, this, new Pubsub.Write(env.getString(OUTPUT_TOPIC), executor, b -> b,
            getOutputCompression(env))::withoutResult);
      }
    },

    gcs {

      @Override
      Output getOutput(Env env, Executor executor) {
        return new Output(env, this,
            new Gcs.Write.Ndjson(getGcsService(env),
                env.getLong(BATCH_MAX_BYTES, DEFAULT_BATCH_MAX_BYTES),
                env.getInt(BATCH_MAX_MESSAGES, DEFAULT_BATCH_MAX_MESSAGES),
                env.getDuration(BATCH_MAX_DELAY, DEFAULT_BATCH_MAX_DELAY),
                PubsubMessageToTemplatedString.of(getGcsOutputBucket(env)), executor,
                getFormat(env), ignore -> CompletableFuture.completedFuture(null))
                    .withOpenCensusMetrics());
      }
    },

    bigQueryLoad {

      @Override
      Output getOutput(Env env, Executor executor) {
        final Storage storage = getGcsService(env);
        final Function<Blob, CompletableFuture<Void>> bigQueryLoad = new BigQuery.Load(
            getBigQueryService(env), storage, env.getLong(LOAD_MAX_BYTES, DEFAULT_LOAD_MAX_BYTES),
            env.getInt(LOAD_MAX_FILES, DEFAULT_LOAD_MAX_FILES),
            env.getDuration(LOAD_MAX_DELAY, DEFAULT_LOAD_MAX_DELAY), executor,
            // don't delete files until successfully loaded
            BigQuery.Load.Delete.onSuccess).withOpenCensusMetrics();
        // Messages may be delivered more than once, so check whether the blob has been deleted.
        // The blob is never deleted in this mode unless it has already been successfully loaded
        // to BigQuery. If the blob does not exist, it must have been deleted, because Cloud
        // Storage provides strong global consistency for read-after-write operations.
        // https://cloud.google.com/storage/docs/consistency
        return new Output(env, this,
            message -> CompletableFuture.completedFuture(message)
                .thenApply(BlobIdToPubsubMessage::decode)
                // ApplyAsync for storage::get because it is a blocking IO operation
                .thenApplyAsync(storage::get, executor).thenCompose(blob -> {
                  if (blob == null) {
                    // blob was deleted, so ack by returning a successfully completed future
                    // TODO measure the frequency of this
                    return CompletableFuture.completedFuture((Void) null);
                  }
                  return bigQueryLoad.apply(blob);
                }));
      }

      // Allow enough outstanding elements to fill one batch per table.
      @Override
      long getDefaultMaxOutstandingElementCount() {
        return 1_500_000L; // 1.5M messages
      }

      @Override
      long getDefaultMaxOutstandingRequestBytes() {
        // Allow enough bytes to reach max outstanding element count. Average bytes per element is
        // expected to be a little under 200 bytes.
        return 300_000_000; // 300MB
      }
    },

    bigQueryFiles {

      @Override
      Output getOutput(Env env, Executor executor) {
        Output pubsubWrite = pubsub.getOutput(env, executor);
        return new Output(env, this,
            new Gcs.Write.Ndjson(getGcsService(env),
                env.getLong(BATCH_MAX_BYTES, DEFAULT_BATCH_MAX_BYTES),
                env.getInt(BATCH_MAX_MESSAGES, DEFAULT_BATCH_MAX_MESSAGES),
                env.getDuration(BATCH_MAX_DELAY, DEFAULT_BATCH_MAX_DELAY),
                PubsubMessageToTemplatedString.forBigQuery(getBigQueryOutputBucket(env)), executor,
                getFormat(env),
                // BigQuery Load API limits maximum load requests per table per day to 1,000 so send
                // blobInfo to pubsub and require loads be run separately to reduce maximum latency
                blobInfo -> pubsubWrite.apply(BlobIdToPubsubMessage.encode(blobInfo.getBlobId())))
                    .withOpenCensusMetrics());
      }
    },

    bigQueryStreaming {

      @Override
      Output getOutput(Env env, Executor executor) {
        return new Output(env, this,
            new BigQuery.Write(getBigQueryService(env),
                env.getLong(BATCH_MAX_BYTES, DEFAULT_STREAMING_BATCH_MAX_BYTES),
                env.getInt(BATCH_MAX_MESSAGES, DEFAULT_STREAMING_BATCH_MAX_MESSAGES),
                env.getDuration(BATCH_MAX_DELAY, DEFAULT_STREAMING_BATCH_MAX_DELAY),
                PubsubMessageToTemplatedString.forBigQuery(env.getString(OUTPUT_TABLE)), executor,
                getFormat(env)).withOpenCensusMetrics());
      }
    },

    bigQueryMixed {

      @Override
      Output getOutput(Env env, Executor executor) {
        final com.google.cloud.bigquery.BigQuery bigQuery = getBigQueryService(env);
        final Storage storage = getGcsService(env);
        final Function<Blob, CompletableFuture<Void>> bigQueryLoad;
        if (env.containsKey(OUTPUT_TOPIC)) {
          // BigQuery Load API limits maximum load requests per table per day to 1,000 so if
          // OUTPUT_TOPIC is present send blobInfo to pubsub and run load jobs separately
          final Function<PubsubMessage, CompletableFuture<Void>> pubsubOutput = pubsub
              .getOutput(env, executor);
          bigQueryLoad = blob -> pubsubOutput.apply(BlobIdToPubsubMessage.encode(blob.getBlobId()));
        } else {
          bigQueryLoad = new BigQuery.Load(bigQuery, storage,
              env.getLong(LOAD_MAX_BYTES, DEFAULT_LOAD_MAX_BYTES),
              env.getInt(LOAD_MAX_FILES, DEFAULT_LOAD_MAX_FILES),
              env.getDuration(LOAD_MAX_DELAY, DEFAULT_STREAMING_LOAD_MAX_DELAY), executor,
              // files will be recreated if not successfully loaded
              BigQuery.Load.Delete.always).withOpenCensusMetrics();
        }
        // Combine bigQueryFiles and bigQueryLoad without an intermediate PubSub topic
        Function<PubsubMessage, CompletableFuture<Void>> fileOutput = new Gcs.Write.Ndjson(storage,
            env.getLong(BATCH_MAX_BYTES, DEFAULT_BATCH_MAX_BYTES),
            env.getInt(BATCH_MAX_MESSAGES, DEFAULT_BATCH_MAX_MESSAGES),
            env.getDuration(BATCH_MAX_DELAY, DEFAULT_BATCH_MAX_DELAY),
            PubsubMessageToTemplatedString.forBigQuery(getBigQueryOutputBucket(env)), executor,
            getFormat(env), bigQueryLoad).withOpenCensusMetrics();
        // Like bigQueryStreaming, but use STREAMING_ prefix env vars for batch configuration
        Function<PubsubMessage, CompletableFuture<Void>> streamingOutput = new BigQuery.Write(
            bigQuery, env.getLong(STREAMING_BATCH_MAX_BYTES, DEFAULT_STREAMING_BATCH_MAX_BYTES),
            env.getInt(STREAMING_BATCH_MAX_MESSAGES, DEFAULT_STREAMING_BATCH_MAX_MESSAGES),
            env.getDuration(STREAMING_BATCH_MAX_DELAY, DEFAULT_STREAMING_BATCH_MAX_DELAY),
            PubsubMessageToTemplatedString.forBigQuery(env.getString(OUTPUT_TABLE)), executor,
            getFormat(env)).withOpenCensusMetrics();
        // fallbackOutput sends messages to fileOutput when rejected by streamingOutput due to size
        Function<PubsubMessage, CompletableFuture<Void>> fallbackOutput = message -> streamingOutput
            .apply(message).thenApply(CompletableFuture::completedFuture).exceptionally(t -> {
              if (t.getCause() instanceof BigQueryErrors) {
                BigQueryErrors cause = (BigQueryErrors) t.getCause();
                if (cause.errors.size() == 1 && cause.errors.get(0).getMessage()
                    .startsWith("Maximum allowed row size exceeded")) {
                  return fileOutput.apply(message);
                }
              } else if (t.getCause() instanceof BigQueryException && t.getCause().getMessage()
                  .startsWith("Request payload size exceeds the limit")) {
                // t.getCause() was not a BatchException, so this message exceeded the
                // request payload size limit when sent individually.
                return fileOutput.apply(message);
              }
              throw (RuntimeException) t;
            }).thenCompose(v -> v);
        // Send messages not matched by STREAMING_DOCTYPES directly to fileOutput
        final Function<PubsubMessage, CompletableFuture<Void>> mixedOutput;
        if (env.containsKey(STREAMING_DOCTYPES)) {
          Predicate<PubsubMessage> streamingDoctypes = DocumentTypePredicate
              .of(env.getPattern(STREAMING_DOCTYPES));
          mixedOutput = message -> {
            if (streamingDoctypes.test(message)) {
              return fallbackOutput.apply(message);
            }
            return fileOutput.apply(message);
          };
        } else {
          mixedOutput = fallbackOutput;
        }
        return new Output(env, this, mixedOutput);
      }
    },

    amplitudeDelete {

      @Override
      Output getOutput(Env env, Executor executor) {
        return new Output(env, this, new Amplitude.Delete(
            // TODO pass initialized amplitude service here
            new Amplitude.Client(env.getString(AMPLITUDE_API_KEY),
                env.getString(AMPLITUDE_SECRET_KEY)),
            env.getLong(BATCH_MAX_BYTES, DEFAULT_STREAMING_BATCH_MAX_BYTES),
            // "Up to 100 users can be specified at a time"
            // https://help.amplitude.com/hc/en-us/articles/360000398191-User-Privacy-API#h_a3b54ec9-27e9-4efa-8133-fce1bbd31800
            env.getInt(BATCH_MAX_MESSAGES, 100),
            // "The endpoint /api/2/deletions/users has a rate limit of 1 HTTP request per second",
            // so batch up for 10 seconds to avoid hitting the rate limit.
            // https://help.amplitude.com/hc/en-us/articles/360000398191-User-Privacy-API#h_2beded8a-5c39-4113-a847-551b7151339b
            env.getDuration(BATCH_MAX_DELAY, "10s"), executor));
      }
    };

    // Each case in the enum must implement this method to define how to write out messages.
    abstract Output getOutput(Env env, Executor executor);

    // Cases in the enum may override this method set a more appropriate default.
    long getDefaultMaxOutstandingElementCount() {
      return 40_000L; // 40K messages
    }

    // Cases in the enum may override this method set a more appropriate default.
    long getDefaultMaxOutstandingRequestBytes() {
      return 30_000_000L; // 30MB
    }

    static OutputType get(Env env) {
      boolean hasBigQueryOutputMode = env.containsKey(BIG_QUERY_OUTPUT_MODE);
      if (env.containsKey(OUTPUT_BUCKET) && !hasBigQueryOutputMode) {
        return OutputType.gcs;
      } else if (env.containsKey(OUTPUT_TOPIC) && !hasBigQueryOutputMode) {
        return OutputType.pubsub;
      } else if (env.containsKey(OUTPUT_TABLE) || hasBigQueryOutputMode) {
        final String outputMode = env.getString(BIG_QUERY_OUTPUT_MODE, "streaming").toLowerCase();
        switch (outputMode) {
          case "streaming":
            return OutputType.bigQueryStreaming;
          case "mixed":
            return OutputType.bigQueryMixed;
          case "file_loads":
            return OutputType.bigQueryFiles;
          default:
            throw new IllegalArgumentException("Unsupported BIG_QUERY_OUTPUT_MODE: " + outputMode);
        }
      } else if (env.containsKey(AMPLITUDE_API_KEY)) {
        return OutputType.amplitudeDelete;
      } else {
        // default to bigQueryLoad because it's the only output without any required configs
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

  private static PubsubMessageToObjectNode getFormat(Env env) {
    final String format = env.getString(OUTPUT_FORMAT, "raw").toLowerCase();
    switch (format) {
      case "raw":
        return PubsubMessageToObjectNode.Raw.of();
      case "decoded":
        return PubsubMessageToObjectNode.Decoded.of();
      case "payload":
        return PubsubMessageToObjectNode.Payload.of(env.getStrings(STRICT_SCHEMA_DOCTYPES, null),
            env.getString(SCHEMAS_LOCATION), FileInputStream::new).withOpenCensusMetrics();
      default:
        throw new IllegalArgumentException("Format not yet implemented: " + format);
    }
  }

  private static DecompressPayload getInputCompression(Env env) {
    return DecompressPayload
        .valueOf(env.getString(INPUT_COMPRESSION, DecompressPayload.NONE.name()).toUpperCase());
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
    // Executor to use for CompletableFutures in outputs instead of ForkJoinPool.commonPool(),
    // because the default parallelism is 1 in stage and prod. Parallelism should be more than one
    // per batch key (i.e. output table), because this executor is used for batch close operations
    // that may be slow synchronous IO. As of 2020-04-25 there are 89 tables for telemetry and 118
    // tables for structured.
    Executor executor = new ForkJoinPool(env.getInt(OUTPUT_PARALLELISM, 150));
    return OutputType.get(env).getOutput(env, executor);
  }

  /** Return a configured input transform. */
  public static Pubsub.Read getInput(Output output) throws IOException {
    // read pubsub messages from INPUT_SUBSCRIPTION
    Pubsub.Read input = new Pubsub.Read(output.env.getString(INPUT_SUBSCRIPTION), output,
        builder -> builder
            .setFlowControlSettings(FlowControlSettings.newBuilder()
                .setMaxOutstandingElementCount(
                    output.type.getMaxOutstandingElementCount(output.env))
                .setMaxOutstandingRequestBytes(
                    output.type.getMaxOutstandingRequestBytes(output.env))
                .build())
            // The number of streaming subscriber connections for reading from Pub/Sub.
            // https://github.com/googleapis/java-pubsub/blob/v1.105.0/google-cloud-pubsub/src/main/java/com/google/cloud/pubsub/v1/Subscriber.java#L141
            // https://github.com/googleapis/java-pubsub/blob/v1.105.0/google-cloud-pubsub/src/main/java/com/google/cloud/pubsub/v1/Subscriber.java#L318-L320
            // The default number of executor threads is max(6, 2*parallelPullCount).
            // https://github.com/googleapis/java-pubsub/blob/v1.105.0/google-cloud-pubsub/src/main/java/com/google/cloud/pubsub/v1/Subscriber.java#L566-L568
            // Subscriber connections are expected to be CPU bound until flow control thresholds are
            // reached, so parallelism should be no less than the number of available processors.
            .setParallelPullCount(
                output.env.getInt(INPUT_PARALLELISM, Runtime.getRuntime().availableProcessors())),
        getInputCompression(output.env));
    output.env.requireAllVarsUsed();
    // Setup OpenCensus stackdriver exporter after all measurement views have been registered,
    // as seen in https://opencensus.io/exporters/supported-exporters/java/stackdriver-stats/
    StackdriverStatsExporter.createAndRegister();
    return input;
  }
}
