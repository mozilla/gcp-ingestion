package com.mozilla.telemetry.ingestion.sink;

import com.mozilla.telemetry.ingestion.sink.config.SinkConfig;

public class Sink {

  private Sink() {
  }

  public static void main(String[] args) {
    SinkConfig.getInput(SinkConfig.getOutput()).run();
  }
}
