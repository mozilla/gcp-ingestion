/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

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
