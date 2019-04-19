/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NormalizeTest {

  /**
   * See a selection of existing raw channel names and their normalized versions in BigQuery.
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
  @Test
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

  @Test
  public void os() {
    // Desktop OS.
    assertEquals("Windows", Normalize.os("Windows"));
    assertEquals("Windows", Normalize.os("WINNT"));
    assertEquals("Windows", Normalize.os("Windows_NT"));
    assertEquals("Windows", Normalize.os("WindowsNT"));
    assertEquals("Mac", Normalize.os("Darwin"));
    assertEquals("Linux", Normalize.os("Linux"));
    assertEquals("Linux", Normalize.os("GNU/Linux"));
    assertEquals("Linux", Normalize.os("SunOS"));
    assertEquals("Linux", Normalize.os("FreeBSD"));
    assertEquals("Linux", Normalize.os("GNU/kFreeBSD"));
    assertEquals("Other", Normalize.os("AIX"));

    // Mobile OS.
    assertEquals("iOS", Normalize.os("iOS"));
    assertEquals("iOS", Normalize.os("iOS?"));
    assertEquals("iOS", Normalize.os("iPhone"));
    assertEquals("iOS", Normalize.os("All the iPhones"));
    assertEquals("Other", Normalize.os("All the iOSes"));
    assertEquals("Other", Normalize.os("IOS"));
    assertEquals("Android", Normalize.os("Android"));
    assertEquals("Android", Normalize.os("Android?"));
    assertEquals("Other", Normalize.os("All the Androids"));

    // Other.
    assertEquals("Other", Normalize.os(null));
    assertEquals("Other", Normalize.os("asdf"));
  }

  /**
   * See a selection of existing raw os names and their normalized versions in BigQuery.
   *
   * <pre>{@code
   * SELECT os_version, normalized_os_version, count(*)
   * FROM `moz-fx-data-derived-datasets.telemetry.clients_daily_v6`
   * WHERE submission_date_s3 = DATE("2019-04-15")
   * group by 1, 2
   * order by 3 desc
   * LIMIT 1000
   * }</pre>
   */
  @Test
  public void osVersion() {
    assertEquals("10.10.3", Normalize.osVersion("10.10.3"));
    assertEquals("10.3", Normalize.osVersion("10.3"));
    assertEquals("10", Normalize.osVersion("10"));
    assertEquals("10.10.3", Normalize.osVersion("10.10.3.4.1"));
    assertEquals("3.10.0", Normalize.osVersion("3.10.0-957.el7.x86_644.15.0"));
    assertEquals("3.13.0", Normalize.osVersion("3.13.0-168-lowlatency"));
    assertEquals("", Normalize.osVersion("hi"));
    assertEquals("", Normalize.osVersion("hi-13.1.3"));
    assertEquals("", Normalize.osVersion(null));
  }

  @Test
  public void appName() {
    assertEquals("Firefox", Normalize.appName("Firefox"));
    assertEquals("Fennec", Normalize.appName("Fennec"));
    assertEquals("Fennec", Normalize.appName("Fennec-x"));
    assertEquals("Focus-TV", Normalize.appName("Focus-TV"));
    assertEquals("Focus", Normalize.appName("Focus"));
    assertEquals("Zerda_cn", Normalize.appName("Zerda_cn"));
    assertEquals("Zerda", Normalize.appName("Zerda"));
    assertEquals("Other", Normalize.appName("asdf"));
    assertEquals("Other", Normalize.appName(null));
  }

  @Test
  public void countryCode() {
    assertEquals("FI", Normalize.countryCode("FI"));
    assertEquals("FI", Normalize.countryCode("fi"));
    assertEquals("Other", Normalize.countryCode("FII"));
    assertEquals("Other", Normalize.countryCode("asdf"));
    assertEquals("Other", Normalize.countryCode(null));
  }

}
