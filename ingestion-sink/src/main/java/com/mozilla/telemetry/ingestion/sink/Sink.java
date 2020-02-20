package com.mozilla.telemetry.ingestion.sink;

import com.mozilla.telemetry.ingestion.sink.config.SinkConfig;
import java.io.IOException;

public class Sink {

  private Sink() {
  }

  public static void main(String[] args) throws IOException {
    SinkConfig.getInput(SinkConfig.getOutput()).run();
  }
}
