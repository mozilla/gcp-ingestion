package com.mozilla.telemetry.ingestion.sink.io;

import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BatchException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Constructor. */
public class WriteWithErrors implements Function<PubsubMessage, CompletableFuture<Void>> {

  private final Function<PubsubMessage, CompletableFuture<Void>> write;
  private final Function<PubsubMessage, CompletableFuture<Void>> errors;
  private final int maxAttempts;

  /** Apply {@code write} until {@code maxAttempts} failures, then apply {@code errors}.
   */
  public WriteWithErrors(Function<PubsubMessage, CompletableFuture<Void>> write,
      Function<PubsubMessage, CompletableFuture<Void>> errors, int maxAttempts) {
    this.write = write;
    this.errors = errors;
    this.maxAttempts = maxAttempts;
  }

  @Override
  public CompletableFuture<Void> apply(PubsubMessage message) {
    return apply(message, 1);
  }

  private CompletableFuture<Void> apply(PubsubMessage message, int attempt) {
    return CompletableFuture.completedFuture(message).thenCompose(write)
        .thenApply(CompletableFuture::completedFuture).exceptionally(t -> {
          if (t.getCause() instanceof BatchException) {
            // batch exceptions don't count toward attempt limit
            return apply(message, attempt);
          }
          if (attempt < maxAttempts) {
            // retry message until maxAttempts is reached
            return apply(message, attempt + 1);
          }
          // deliver to dead letter queue, if provided
          return errors.apply(message);
        }).thenCompose(v -> v);
  }
}
