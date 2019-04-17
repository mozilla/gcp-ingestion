/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NormalizeTest {

  @Test
  /**
   * See a selection of existing raw channel names and their normalized versions in BigQuery:
   *
   * <pre>{@code
   * SELECT channel, normalized_channel, count(*)
   * FROM `moz-fx-data-derived-datasets.telemetry.clients_daily_v6`
   * WHERE true
   *   and submission_date_s3 >= DATE('2018-07-01')
   *   and normalized_channel != 'Other'
   * group by 1, 2
   * order by 3 desc
   * LIMIT 1000
   * }</pre>
   */
  public void channel() {
    assertEquals("release", Normalize.channel("release"));
    assertEquals("esr", Normalize.channel("esr"));
    assertEquals("beta", Normalize.channel("beta"));
    assertEquals("aurora", Normalize.channel("aurora"));
    assertEquals("nightly", Normalize.channel("nightly"));

    assertEquals("nightly", Normalize.channel("nightly-cck-ubuntu"));

    assertEquals("Other", Normalize.channel("no"));
    assertEquals("Other", Normalize.channel("esr52"));
    assertEquals("Other", Normalize.channel("Release"));
    assertEquals("Other", Normalize.channel("Prefeitura Municipal de Santos"));
    assertEquals("Other", Normalize.channel("no"));
    assertEquals("Other", Normalize.channel(null));

    assertEquals("beta", Normalize.channel("beta-cdntest"));
    assertEquals("beta", Normalize.channel("betastop"));
  }
}
