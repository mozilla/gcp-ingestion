package com.mozilla.telemetry.ingestion.sink.util;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;

/**
 * A non-blocking Future that will complete after some amount of time has passed.
 */
public class TimedFuture extends CompletableFuture<Void> {

  public TimedFuture(Duration delay) {
    new Timer().schedule(new CompleteTask(), delay.toMillis());
  }

  private class CompleteTask extends TimerTask {

    @Override
    public void run() {
      complete(null);
    }
  }
}
