package com.mozilla.telemetry.ingestion.sink.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

public class CompletableFutures {

  public static <T> CompletableFuture<Void> allOf(List<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}));
  }

  public static <T> CompletableFuture<T> orNull(CompletableFuture<T> future) {
    return future.exceptionally(t -> null);
  }

  public static <T> CompletableFuture<Void> allExceptions(List<CompletableFuture<T>> futures) {
    return allOf(futures.stream().map(CompletableFutures::orNull).collect(Collectors.toList()));
  }

  /** Join futures and throw the first exception after all futures are complete. */
  public static <T> void joinAllThenThrow(List<CompletableFuture<T>> futures) {
    try {
      // wait for the first exception
      allOf(futures).join();
    } catch (CompletionException e) {
      // wait for all exceptions
      allExceptions(futures);
      // throw first exception
      throw (RuntimeException) e.getCause();
    }
  }
}
