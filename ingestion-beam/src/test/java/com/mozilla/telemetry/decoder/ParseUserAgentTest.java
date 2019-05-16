/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class ParseUserAgentTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String BROWSER = "user_agent_browser";
  private static final String OS = "user_agent_os";
  private static final String VERSION = "user_agent_version";

  private static final Map<String, Map<String, String>> osSingleMatch = new ImmutableMap //
      .Builder<String, Map<String, String>>()
          .put("Firefox AndroidSync", ImmutableMap.of(BROWSER, "FxSync", OS, "Android"))
          .put("Firefox-iOS-Sync", ImmutableMap.of(BROWSER, "FxSync", OS, "iOS"))
          .put("iPod", ImmutableMap.of(OS, "iPod")) //
          .put("iPad", ImmutableMap.of(OS, "iPad")) //
          .put("iPhone", ImmutableMap.of(OS, "iPhone"))
          .put("Android", ImmutableMap.of(OS, "Android"))
          .put("BlackBerry", ImmutableMap.of(OS, "BlackBerry"))
          .put("Macintosh", ImmutableMap.of(OS, "Macintosh"))
          .put("Mozilla/5.0 (Mobile;", ImmutableMap.of(OS, "FirefoxOS"))
          .put("Windows NT 10.0", ImmutableMap.of(OS, "Windows 10"))
          .put("Windows NT 6.3", ImmutableMap.of(OS, "Windows 8.1"))
          .put("Windows NT 6.2", ImmutableMap.of(OS, "Windows 8"))
          .put("Windows NT 6.1", ImmutableMap.of(OS, "Windows 7"))
          .put("Windows NT 6.0", ImmutableMap.of(OS, "Windows Vista"))
          .put("Windows NT 5.1", ImmutableMap.of(OS, "Windows XP"))
          .put("Windows NT 5.0", ImmutableMap.of(OS, "Windows 2000")).build();

  private static final List<String> osPriority = new ArrayList<>(osSingleMatch.keySet());
  private static final List<List<String>> osBestMatch = IntStream.range(0, osPriority.size() - 1)
      .mapToObj(i -> osPriority.subList(i, osPriority.size() - 1)).collect(Collectors.toList());

  private static final Map<String, Map<String, String>> browserSingleMatch = new ImmutableMap //
      .Builder<String, Map<String, String>>()
          .put("Edge/1", ImmutableMap.of(BROWSER, "Edge", VERSION, "1"))
          .put("Edge", ImmutableMap.of(BROWSER, "Edge"))
          .put("Chrome/2", ImmutableMap.of(BROWSER, "Chrome", VERSION, "2"))
          .put("Chrome", ImmutableMap.of(BROWSER, "Chrome"))
          .put("Opera Mini", ImmutableMap.of(BROWSER, "Opera Mini"))
          .put("Opera Mobi", ImmutableMap.of(BROWSER, "Opera Mobi"))
          .put("Opera", ImmutableMap.of(BROWSER, "Opera"))
          .put("MSIE 3", ImmutableMap.of(BROWSER, "MSIE", VERSION, "3"))
          .put("MSIE", ImmutableMap.of(BROWSER, "MSIE"))
          .put("Trident/7.0", ImmutableMap.of(BROWSER, "MSIE", VERSION, "11"))
          .put("Safari", ImmutableMap.of(BROWSER, "Safari"))
          .put("Firefox AndroidSync 4",
              ImmutableMap.of(BROWSER, "FxSync", VERSION, "4", OS, "Android"))
          .put("Firefox AndroidSync", ImmutableMap.of(BROWSER, "FxSync", OS, "Android"))
          .put("Firefox-iOS-Sync/5", ImmutableMap.of(BROWSER, "FxSync", VERSION, "5", OS, "iOS"))
          .put("Firefox-iOS-Sync", ImmutableMap.of(BROWSER, "FxSync", OS, "iOS"))
          .put("Firefox/6", ImmutableMap.of(BROWSER, "Firefox", VERSION, "6"))
          .put("Firefox", ImmutableMap.of(BROWSER, "Firefox")).build();

  private static final List<String> browserPriority = new ArrayList<>(browserSingleMatch.keySet());
  private static final List<List<String>> browserBestMatch = IntStream
      .range(0, browserPriority.size() - 1)
      .mapToObj(i -> browserPriority.subList(i, browserPriority.size() - 1))
      .collect(Collectors.toList());

  private void test(Map<String, Map<String, String>> expect) {
    List<String> input = expect.keySet().stream()
        .map(v -> "{\"attributeMap\":{\"user_agent\":\"" + v + "\"},\"payload\":\"\"}")
        .collect(Collectors.toList());
    List<String> expected = expect.values().stream()
        .map(v -> "{\"attributeMap\":{" + String.join(",",
            v.entrySet().stream().map(e -> "\"" + e.getKey() + "\":\"" + e.getValue() + "\"")
                .collect(Collectors.toList()))
            + "},\"payload\":\"\"}")
        .collect(Collectors.toList());

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(ParseUserAgent.of()) //
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  private void test(List<String> keys, List<Map<String, String>> values) {
    ImmutableMap.Builder<String, Map<String, String>> builder = new ImmutableMap.Builder<>();
    for (int i = 0; i < keys.size(); i++) {
      builder = builder.put(keys.get(i), values.get(i));
    }
    test(builder.build());
  }

  @Test
  public void testBrowserSingleMatch() {
    test(browserSingleMatch);
  }

  @Test
  public void testBrowserBestMatcFirst() {
    test(browserBestMatch.stream().map(v -> String.join("", v)).collect(Collectors.toList()),
        browserSingleMatch.entrySet().stream().map(e -> {
          switch (e.getKey()) {
            case "Safari":
            case "Opera":
            case "Opera Mini":
            case "Opera Mobi":
              return ImmutableMap.of(BROWSER, e.getKey(), VERSION, "6");
            default:
              return e.getValue();
          }
        }).collect(Collectors.toList()));
  }

  @Test
  public void testBrowserBestMatchLast() {
    test(browserBestMatch.stream().map(l -> String.join("", Lists.reverse(l)))
        .collect(Collectors.toList()), browserSingleMatch.entrySet().stream().map(e -> {
          String version = null;
          switch (e.getKey()) {
            case "Safari":
              return ImmutableMap.of(BROWSER, e.getKey(), VERSION, "5");
            case "Opera":
            case "Opera Mini":
            case "Opera Mobi":
              return ImmutableMap.of(BROWSER, e.getKey(), VERSION, "7");
            default:
              return e.getValue();
          }
        }).collect(Collectors.toList()));
  }

  @Test
  public void testOsSingleMatch() {
    test(osSingleMatch);
  }

  @Test
  public void testOsBestMatchFirst() {
    test(osBestMatch.stream().map(l -> String.join("", l)).collect(Collectors.toList()),
        new ArrayList<>(osSingleMatch.values()));
  }

  @Test
  public void testOsBestMatchLast() {
    test(osBestMatch.stream().map(l -> String.join("", Lists.reverse(l)))
        .collect(Collectors.toList()), new ArrayList<>(osSingleMatch.values()));
  }
}
