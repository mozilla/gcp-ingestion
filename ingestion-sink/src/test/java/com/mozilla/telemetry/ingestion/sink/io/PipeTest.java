package com.mozilla.telemetry.ingestion.sink.io;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class PipeTest {

  @Test
  public void canStopAsync() {
    Input input = Pipe.Read.of(new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8)), m -> {
      throw new AssertionError("unreachable");
    }, m -> m);
    input.run();
    input.stopAsync();
  }
}
