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
import com.mozilla.telemetry.ingestion.core.transform.PubsubMessageToObjectNode;
import com.mozilla.telemetry.ingestion.sink.io.BigQuery;
import com.mozilla.telemetry.ingestion.sink.io.BigQuery.BigQueryErrors;
import com.mozilla.telemetry.ingestion.sink.io.Gcs;
import com.mozilla.telemetry.ingestion.sink.io.Input;
import com.mozilla.telemetry.ingestion.sink.io.Pipe;
import com.mozilla.telemetry.ingestion.sink.io.Pubsub;
import com.mozilla.telemetry.ingestion.sink.io.PubsubLite;
import com.mozilla.telemetry.ingestion.sink.io.WriteWithErrors;
import com.mozilla.telemetry.ingestion.sink.transform.BlobIdToPubsubMessage;
import com.mozilla.telemetry.ingestion.sink.transform.CompressPayload;
import com.mozilla.telemetry.ingestion.sink.transform.DecompressPayload;
import com.mozilla.telemetry.ingestion.sink.transform.DocumentTypePredicate;
import com.mozilla.telemetry.ingestion.sink.transform.PubsubMessageToTemplatedString;
import com.mozilla.telemetry.ingestion.sink.util.Env;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Predicate;

public class SinkConfig {

  private SinkConfig() {
  }

  private static final String INPUT_COMPRESSION = "INPUT_COMPRESSION";
  private static final String INPUT_PARALLELISM = "INPUT_PARALLELISM";
  private static final String INPUT_PIPE = "INPUT_PIPE";
  private static final String INPUT_SUBSCRIPTION = "INPUT_SUBSCRIPTION";
  private static final String MAX_OUTSTANDING_ELEMENT_COUNT = "MAX_OUTSTANDING_ELEMENT_COUNT";
  private static final String MAX_OUTSTANDING_REQUEST_BYTES = "MAX_OUTSTANDING_REQUEST_BYTES";

  private static final Set<String> INPUT_ENV_VARS = ImmutableSet.of(INPUT_COMPRESSION,
      INPUT_PARALLELISM, INPUT_PIPE, INPUT_SUBSCRIPTION, MAX_OUTSTANDING_ELEMENT_COUNT,
      MAX_OUTSTANDING_REQUEST_BYTES);

  public static final String OUTPUT_TABLE = "OUTPUT_TABLE";

  private static final String BATCH_MAX_BYTES = "BATCH_MAX_BYTES";
  private static final String BATCH_MAX_DELAY = "BATCH_MAX_DELAY";
  private static final String BATCH_MAX_MESSAGES = "BATCH_MAX_MESSAGES";
  private static final String BIG_QUERY_OUTPUT_MODE = "BIG_QUERY_OUTPUT_MODE";
  private static final String BIG_QUERY_DEFAULT_PROJECT = "BIG_QUERY_DEFAULT_PROJECT";
  private static final String LOAD_MAX_BYTES = "LOAD_MAX_BYTES";
  private static final String LOAD_MAX_DELAY = "LOAD_MAX_DELAY";
  private static final String LOAD_MAX_FILES = "LOAD_MAX_FILES";
  private static final String OUTPUT_BUCKET = "OUTPUT_BUCKET";
  private static final String OUTPUT_COMPRESSION = "OUTPUT_COMPRESSION";
  private static final String OUTPUT_FORMAT = "OUTPUT_FORMAT";
  private static final String OUTPUT_MAX_ATTEMPTS = "OUTPUT_MAX_ATTEMPTS";
  private static final String OUTPUT_PARALLELISM = "OUTPUT_PARALLELISM";
  private static final String OUTPUT_PIPE = "OUTPUT_PIPE";
  private static final String OUTPUT_TOPIC = "OUTPUT_TOPIC";
  private static final String SCHEMAS_LOCATION = "SCHEMAS_LOCATION";
  private static final String STREAMING_BATCH_MAX_BYTES = "STREAMING_BATCH_MAX_BYTES";
  private static final String STREAMING_BATCH_MAX_DELAY = "STREAMING_BATCH_MAX_DELAY";
  private static final String STREAMING_BATCH_MAX_MESSAGES = "STREAMING_BATCH_MAX_MESSAGES";
  private static final String STREAMING_DOCTYPES = "STREAMING_DOCTYPES";
  private static final String STRICT_SCHEMA_DOCTYPES = "STRICT_SCHEMA_DOCTYPES";

  private static final Set<String> OUTPUT_ENV_VARS = ImmutableSet.of(BATCH_MAX_BYTES,
      BATCH_MAX_DELAY, BATCH_MAX_MESSAGES, BIG_QUERY_OUTPUT_MODE, BIG_QUERY_DEFAULT_PROJECT,
      LOAD_MAX_BYTES, LOAD_MAX_DELAY, LOAD_MAX_FILES, OUTPUT_BUCKET, OUTPUT_COMPRESSION,
      OUTPUT_FORMAT, OUTPUT_MAX_ATTEMPTS, OUTPUT_PARALLELISM, OUTPUT_PIPE, OUTPUT_TABLE,
      OUTPUT_TOPIC, SCHEMAS_LOCATION, STREAMING_BATCH_MAX_BYTES, STREAMING_BATCH_MAX_DELAY,
      STREAMING_BATCH_MAX_MESSAGES, STREAMING_DOCTYPES, STRICT_SCHEMA_DOCTYPES);

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
  // BatchExceptions don't count toward this limit. Only used when error output is configured,
  // otherwise messages will be immediately nacked on failure.
  private static final int DEFAULT_OUTPUT_MAX_ATTEMPTS = 3;

  @VisibleForTesting
  public static class Output implements Function<PubsubMessage, CompletableFuture<Void>> {

    private final OutputType type;
    private final Function<PubsubMessage, CompletableFuture<Void>> write;

    private Output(OutputType outputType, Function<PubsubMessage, CompletableFuture<Void>> write) {
      this.type = outputType;
      this.write = write;
    }

    public Output via(Function<PubsubMessage, CompletableFuture<Void>> write) {
      return new Output(this.type, write);
    }

    public Output errorsVia(Function<PubsubMessage, CompletableFuture<Void>> errors,
        int maxAttempts) {
      return new Output(this.type, new WriteWithErrors(write, errors, maxAttempts));
    }

    public CompletableFuture<Void> apply(PubsubMessage message) {
      return write.apply(message);
    }
  }

  enum OutputType {
    pipe {

      @Override
      Output getOutput(Env env, Executor executor) {
        final String outputPipe = env.getString(OUTPUT_PIPE);
        final PrintStream pipe;
        switch (outputPipe) {
          case "-":
          case "1":
          case "out":
          case "stdout":
          case "/dev/stdout":
            pipe = System.out;
            break;
          case "2":
          case "err":
          case "stderr":
          case "/dev/stderr":
            pipe = System.err;
            break;
          default:
            try {
              pipe = new PrintStream(outputPipe);
            } catch (FileNotFoundException e) {
              throw new IllegalArgumentException(e);
            }
        }
        return new Output(this, Pipe.Write.of(pipe, env.optString(OUTPUT_TABLE)
            .map(PubsubMessageToTemplatedString::forBigQuery).orElse(null), getFormat(env)));
      }
    },

    pubsub {

      private CompressPayload getOutputCompression(Env env) {
        return CompressPayload.valueOf(
            env.getString(OUTPUT_COMPRESSION, CompressPayload.NONE.toString()).toUpperCase());
      }

      @Override
      Output getOutput(Env env, Executor executor) {
        return new Output(this, new Pubsub.Write(env.getString(OUTPUT_TOPIC), executor, b -> b,
            getOutputCompression(env))::withoutResult);
      }
    },

    gcs {

      @Override
      Output getOutput(Env env, Executor executor) {
        return new Output(this,
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
        return new Output(this,
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

      @Override
      boolean validErrorOutput() {
        return false;
      }
    },

    bigQueryFiles {

      @Override
      Output getOutput(Env env, Executor executor) {
        Output pubsubWrite = pubsub.getOutput(env, executor);
        return new Output(this,
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
        return new Output(this,
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
        return new Output(this, mixedOutput);
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

    // Cases in the enum override this method to return false when they are not valid error outputs.
    boolean validErrorOutput() {
      return true;
    }

    static OutputType get(Env env) {
      boolean hasBigQueryOutputMode = env.containsKey(BIG_QUERY_OUTPUT_MODE);
      if (env.containsKey(OUTPUT_PIPE)) {
        return OutputType.pipe;
      } else if (env.containsKey(OUTPUT_BUCKET) && !hasBigQueryOutputMode) {
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
            env.getString(SCHEMAS_LOCATION, null), FileInputStream::new).withOpenCensusMetrics();
      case "beam":
        return PubsubMessageToObjectNode.Beam.of();
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

  @VisibleForTesting
  static com.google.cloud.bigquery.BigQuery getBigQueryService(Env env) {
    BigQueryOptions.Builder builder = BigQueryOptions.getDefaultInstance().toBuilder();
    env.optString(BIG_QUERY_DEFAULT_PROJECT).ifPresent(builder::setProjectId);
    return builder.build().getService();
  }

  private static Storage getGcsService(Env env) {
    return StorageOptions.getDefaultInstance().getService();
  }

  /** Return a configured output transform. */
  public static Output getOutput() {
    final Env outputEnv = new Env(OUTPUT_ENV_VARS);
    final Env errorEnv = new Env(OUTPUT_ENV_VARS, "ERROR_");
    // Executor to use for CompletableFutures in outputs instead of ForkJoinPool.commonPool(),
    // because the default parallelism is 1 in stage and prod. Parallelism should be more than one
    // per batch key (i.e. output table), because this executor is used for batch close operations
    // that may be slow synchronous IO. As of 2020-04-25 there are 89 tables for telemetry and 118
    // tables for structured.
    final Executor executor = new ForkJoinPool(outputEnv.getInt(OUTPUT_PARALLELISM, 150));
    final Output output = OutputType.get(outputEnv).getOutput(outputEnv, executor);

    // Handle error output if configured.
    final Output withErrors;
    if (outputEnv.containsKey(OUTPUT_MAX_ATTEMPTS) || !errorEnv.isEmpty()) {
      OutputType errorOutputType = OutputType.get(errorEnv);
      if (errorOutputType.validErrorOutput()) {
        withErrors = output.errorsVia(errorOutputType.getOutput(errorEnv, executor).write,
            outputEnv.getInt(OUTPUT_MAX_ATTEMPTS, DEFAULT_OUTPUT_MAX_ATTEMPTS));
      } else {
        throw new IllegalArgumentException(
            "Invalid error output type detected: " + errorOutputType.name());
      }
    } else {
      withErrors = output;
    }

    outputEnv.requireAllVarsUsed();
    errorEnv.requireAllVarsUsed();
    return withErrors;
  }

  /** Return a configured input transform. */
  public static Input getInput(Output output) throws IOException {
    final Env env = new Env(INPUT_ENV_VARS);
    final DecompressPayload decompress = getInputCompression(env);
    final Input input;
    if (env.containsKey(INPUT_PIPE)) {
      final String inputPipe = env.getString(INPUT_PIPE);
      final InputStream pipe;
      switch (inputPipe) {
        case "-":
        case "0":
        case "in":
        case "stdin":
        case "/dev/stdin":
          pipe = System.in;
          break;
        default:
          pipe = Files.newInputStream(Paths.get(inputPipe));
      }
      input = Pipe.Read.of(pipe, output.write, decompress);
    } else {
      final String subscription = env.getString(INPUT_SUBSCRIPTION);
      final long messagesOutstanding = output.type.getMaxOutstandingElementCount(env);
      final long bytesOutstanding = output.type.getMaxOutstandingRequestBytes(env);
      // Pub/Sub Lite subscriptions specify a zone, otherwise it is a standard Pub/Sub subscription
      if (subscription.matches(".*/locations/.*")) {
        input = new PubsubLite.Read(subscription, messagesOutstanding, bytesOutstanding,
            output.write, builder -> builder, decompress);
      } else {
        input = new Pubsub.Read(subscription, output, builder -> builder
            .setFlowControlSettings(
                FlowControlSettings.newBuilder().setMaxOutstandingElementCount(messagesOutstanding)
                    .setMaxOutstandingRequestBytes(bytesOutstanding).build())
            // The number of streaming subscriber connections for reading from Pub/Sub.
            // https://github.com/googleapis/java-pubsub/blob/v1.105.0/google-cloud-pubsub/src/main/java/com/google/cloud/pubsub/v1/Subscriber.java#L141
            // https://github.com/googleapis/java-pubsub/blob/v1.105.0/google-cloud-pubsub/src/main/java/com/google/cloud/pubsub/v1/Subscriber.java#L318-L320
            // The default number of executor threads is max(6, 2*parallelPullCount).
            // https://github.com/googleapis/java-pubsub/blob/v1.105.0/google-cloud-pubsub/src/main/java/com/google/cloud/pubsub/v1/Subscriber.java#L566-L568
            // Subscriber connections are expected to be CPU bound until flow control thresholds are
            // reached, so parallelism should be no less than the number of available processors.
            .setParallelPullCount(
                env.getInt(INPUT_PARALLELISM, Runtime.getRuntime().availableProcessors())),
            decompress);
      }
      // Setup OpenCensus stackdriver exporter after all measurement views have been registered,
      // as seen in https://opencensus.io/exporters/supported-exporters/java/stackdriver-stats/
      StackdriverStatsExporter.createAndRegister();
    }
    env.requireAllVarsUsed();
    return input;
  }
}
