package com.mozilla.telemetry.ingestion.core.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

@FunctionalInterface
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public interface IOFunction<T, R> {

  R apply(T input) throws IOException;

  /**
   * Turn an {@link IOFunction} into a {@link Function} by throwing {@link UncheckedIOException}.
   */
  static <T, R> Function<T, R> unchecked(IOFunction<T, R> ioFunction) {
    return (T input) -> {
      try {
        return ioFunction.apply(input);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }
}
