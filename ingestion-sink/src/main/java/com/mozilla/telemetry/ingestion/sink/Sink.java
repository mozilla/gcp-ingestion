package com.mozilla.telemetry.ingestion.sink;

import com.google.common.annotations.VisibleForTesting;
import com.mozilla.telemetry.ingestion.sink.config.SinkConfig;
import com.mozilla.telemetry.ingestion.sink.io.Pubsub;

public class Sink {

  private Sink() {
  }

  @VisibleForTesting
  static Pubsub.Read main() {
    return SinkConfig.getInput();
  }

  public static void main(String[] args) {
    main().run(); // run pubsub reader
  }
}
