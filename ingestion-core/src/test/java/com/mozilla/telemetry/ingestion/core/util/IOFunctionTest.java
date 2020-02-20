package com.mozilla.telemetry.ingestion.core.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.junit.Test;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class IOFunctionTest {

  Function<String, String> unchecked = IOFunction.unchecked(m -> {
    if (m.equals("throw")) {
      throw new IOException(m);
    }
    return m;
  });

  @Test(expected = UncheckedIOException.class)
  public void canRethrowException() {
    unchecked.apply("throw");
  }

  @Test
  public void canApplyUnchecked() {
    assertEquals("test", CompletableFuture.completedFuture("test").thenApply(unchecked).join());
  }
}
