package com.mozilla.telemetry.ingestion.sink.io;

public interface Input {

  void run();

  default void stopAsync() {
  }
}
