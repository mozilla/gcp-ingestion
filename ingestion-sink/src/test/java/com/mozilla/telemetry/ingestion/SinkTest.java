/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SinkTest {

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test(expected = IllegalArgumentException.class)
  public void failsOnMissingInput() {
    environmentVariables.set("OUTPUT_TABLE", "dataset.table");
    Sink.main(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnMissingOutput() {
    environmentVariables.set("INPUT_SUBSCRIPTION", "projects/test/subscriptions/test");
    Sink.main(null);
  }
}
