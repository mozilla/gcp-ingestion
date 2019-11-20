package com.mozilla.telemetry.util;

import static org.junit.Assert.assertEquals;

import com.mozilla.telemetry.transforms.NormalizeAttributes;
import java.util.Optional;
import org.junit.Test;

public class NormalizeAttributesTest {

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
    assertEquals("release", NormalizeAttributes.normalizeChannel("release"));
    assertEquals("esr", NormalizeAttributes.normalizeChannel("esr"));
    assertEquals("beta", NormalizeAttributes.normalizeChannel("beta"));
    assertEquals("aurora", NormalizeAttributes.normalizeChannel("aurora"));
    assertEquals("nightly", NormalizeAttributes.normalizeChannel("nightly"));

    assertEquals("nightly", NormalizeAttributes.normalizeChannel("nightly-cck-ubuntu"));

    assertEquals("Other", NormalizeAttributes.normalizeChannel("no"));
    assertEquals("Other", NormalizeAttributes.normalizeChannel("esr52"));
    assertEquals("Other", NormalizeAttributes.normalizeChannel("Release"));
    assertEquals("Other", NormalizeAttributes.normalizeChannel("Prefeitura Municipal de Santos"));
    assertEquals("Other", NormalizeAttributes.normalizeChannel("no"));

    assertEquals("beta", NormalizeAttributes.normalizeChannel("beta-cdntest"));
    assertEquals("beta", NormalizeAttributes.normalizeChannel("betastop"));
  }

  @Test
  public void os() {
    // Desktop OS.
    assertEquals("Windows", NormalizeAttributes.normalizeOs("Windows", Optional.of("Firefox")));
    assertEquals("Windows", NormalizeAttributes.normalizeOs("WINNT", Optional.of("Firefox")));
    assertEquals("Windows", NormalizeAttributes.normalizeOs("Windows_NT", Optional.of("Firefox")));
    assertEquals("Windows", NormalizeAttributes.normalizeOs("WindowsNT", Optional.of("Firefox")));
    assertEquals("Windows", NormalizeAttributes.normalizeOs("Windows", Optional.of("Other")));
    assertEquals("Mac", NormalizeAttributes.normalizeOs("Darwin", Optional.of("Firefox")));
    assertEquals("Linux", NormalizeAttributes.normalizeOs("Linux", Optional.of("Firefox")));
    assertEquals("Linux", NormalizeAttributes.normalizeOs("GNU/Linux", Optional.of("Firefox")));
    assertEquals("Linux", NormalizeAttributes.normalizeOs("SunOS", Optional.of("Firefox")));
    assertEquals("Linux", NormalizeAttributes.normalizeOs("FreeBSD", Optional.of("Firefox")));
    assertEquals("Linux", NormalizeAttributes.normalizeOs("GNU/kFreeBSD", Optional.of("Firefox")));
    assertEquals("Other", NormalizeAttributes.normalizeOs("AIX", Optional.of("Firefox")));

    // Not Desktop OS.
    assertEquals("iOS", NormalizeAttributes.normalizeOs("iOS", Optional.of("Fennec")));
    assertEquals("iOS", NormalizeAttributes.normalizeOs("iOS?", Optional.of("Fennec")));
    assertEquals("iOS", NormalizeAttributes.normalizeOs("iPhone", Optional.of("Fennec")));
    assertEquals("iOS", NormalizeAttributes.normalizeOs("All the iPhones", Optional.of("Fennec")));
    assertEquals("Other", NormalizeAttributes.normalizeOs("All the iOSes", Optional.of("Fennec")));
    assertEquals("Other", NormalizeAttributes.normalizeOs("IOS", Optional.of("Fennec")));
    assertEquals("Android", NormalizeAttributes.normalizeOs("Android", Optional.of("Fennec")));
    assertEquals("Android", NormalizeAttributes.normalizeOs("Android?", Optional.of("Fennec")));
    assertEquals("Android", NormalizeAttributes.normalizeOs("Android Linux", Optional.of("Fennec")));
    assertEquals("Linux", NormalizeAttributes.normalizeOs("Linux", Optional.of("Fennec")));
    assertEquals("Other", NormalizeAttributes.normalizeOs("Windows", Optional.of("Fennec")));
    assertEquals("Other",
        NormalizeAttributes.normalizeOs("All the Androids", Optional.of("Fennec")));

    // Other.
    assertEquals("Other", NormalizeAttributes.normalizeOs("asdf", Optional.empty()));
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
    assertEquals("10.10.3", NormalizeAttributes.normalizeOsVersion("10.10.3"));
    assertEquals("10.3", NormalizeAttributes.normalizeOsVersion("10.3"));
    assertEquals("10", NormalizeAttributes.normalizeOsVersion("10"));
    assertEquals("10.10.3", NormalizeAttributes.normalizeOsVersion("10.10.3.4.1"));
    assertEquals("3.10.0", NormalizeAttributes.normalizeOsVersion("3.10.0-957.el7.x86_644.15.0"));
    assertEquals("3.13.0", NormalizeAttributes.normalizeOsVersion("3.13.0-168-lowlatency"));
    assertEquals("", NormalizeAttributes.normalizeOsVersion("hi"));
    assertEquals("", NormalizeAttributes.normalizeOsVersion("hi-13.1.3"));
  }

  @Test
  public void appName() {
    assertEquals("Firefox", NormalizeAttributes.normalizeAppName("Firefox"));
    assertEquals("Fennec", NormalizeAttributes.normalizeAppName("Fennec"));
    assertEquals("Fennec", NormalizeAttributes.normalizeAppName("Fennec-x"));
    assertEquals("Focus-TV", NormalizeAttributes.normalizeAppName("Focus-TV"));
    assertEquals("Focus", NormalizeAttributes.normalizeAppName("Focus"));
    assertEquals("Zerda_cn", NormalizeAttributes.normalizeAppName("Zerda_cn"));
    assertEquals("Zerda", NormalizeAttributes.normalizeAppName("Zerda"));
    assertEquals("Other", NormalizeAttributes.normalizeAppName("asdf"));
  }

  @Test
  public void countryCode() {
    assertEquals("FI", NormalizeAttributes.normalizeCountryCode("FI"));
    assertEquals("FI", NormalizeAttributes.normalizeCountryCode("fi"));
    assertEquals("Other", NormalizeAttributes.normalizeCountryCode("FII"));
    assertEquals("Other", NormalizeAttributes.normalizeCountryCode("asdf"));
  }

}
