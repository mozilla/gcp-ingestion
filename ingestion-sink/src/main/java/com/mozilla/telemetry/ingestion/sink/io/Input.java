package com.mozilla.telemetry.ingestion.sink.io;

public interface Input {

  void stop();

  void run();
}
