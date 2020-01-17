package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Extracts browser, version and os information from a user_agent attribute.
 */
public class ParseUserAgent
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static ParseUserAgent of() {
    return INSTANCE;
  }

  ////////

  private enum Os {
    // Define os's in descending order of best match
    IPOD("iPod"), //
    IPAD("iPad"), //
    IPHONE("iPhone"), //
    ANDROID("Android"), //
    BLACKBERRY("BlackBerry"), //
    LINUX("Linux"), //
    MACINTOSH("Macintosh"), //
    FIREFOXOS("FirefoxOS", "Mozilla/5\\.0 \\(Mobile;"), //
    WINDOWS_10("Windows 10", "Windows NT 10\\.0"), //
    WINDOWS_8_1("Windows 8.1", "Windows NT 6\\.3"), //
    WINDOWS_8("Windows 8", "Windows NT 6\\.2"), //
    WINDOWS_7("Windows 7", "Windows NT 6\\.1"), //
    WINDOWS_VISTA("Windows Vista", "Windows NT 6\\.0"), //
    WINDOWS_XP("Windows XP", "Windows NT 5\\.1"), //
    WINDOWS_2000("Windows 2000", "Windows NT 5\\.0");

    final String pattern; // pattern is the regex that indicates a match
    final String value; // value is what gets set in attributes

    Os(String value) {
      this.value = value;
      this.pattern = value;
    }

    Os(String value, String pattern) {
      this.value = value;
      this.pattern = pattern;
    }

    static Map<String, Os> asMap() {
      ImmutableMap.Builder<String, Os> builder = new ImmutableMap.Builder<>();
      for (Os os : Os.values()) {
        builder = builder.put(os.pattern.replace("\\", ""), os);
      }
      return builder.build();
    }
  }

  private enum Browser {
    // Define browsers in descending order of best match
    EDGE("Edge", "/"), //
    CHROME("Chrome", "/"), //
    OPERA_MINI("Opera Mini", LAST_SLASH), //
    OPERA_MOBI("Opera Mobi", LAST_SLASH), //
    OPERA("Opera", ".*/"), //
    MSIE("MSIE", " "), //
    TRIDENT_7(Browser.MSIE.value, null, "Trident/7\\.0", null, "11"), //
    SAFARI("Safari", LAST_SLASH), //
    FX_ANDROID_SYNC("FxSync", " ", "Firefox AndroidSync", Os.ANDROID.value), //
    FX_IOS_SYNC(Browser.FX_ANDROID_SYNC.value, "/", "Firefox-iOS-Sync", "iOS"), //
    FIREFOX("Firefox", "/");

    final String value; // string to set in attributes
    final String separator; // regex that separates browser from version
    final String pattern; // regex that indicates a match
    final String os; // override for os
    final String version; // override for version

    Browser(String value, String separator) {
      this.value = value;
      this.separator = separator;
      this.pattern = value;
      this.os = null;
      this.version = null;
    }

    Browser(String value, String separator, String pattern, String os) {
      this.value = value;
      this.separator = separator;
      this.pattern = pattern;
      this.os = os;
      this.version = null;
    }

    Browser(String value, String separator, String pattern, String os, String version) {
      this.value = value;
      this.separator = separator;
      this.pattern = pattern;
      this.os = os;
      this.version = version;
    }

    static Map<String, Browser> asMap() {
      ImmutableMap.Builder<String, Browser> builder = new ImmutableMap.Builder<>();
      for (Browser browser : Browser.values()) {
        builder = builder.put(browser.pattern.replace("\\", ""), browser);
      }
      return builder.build();
    }
  }

  private static Pattern compileRegex() {
    // Extract lists of patterns by separator from Browser.values()
    Map<String, List<String>> versionLookbacks = new HashMap<>();
    for (Browser browser : Browser.values()) {
      // browser.separator is null when the pattern
      if (browser.separator != null && browser.separator != LAST_SLASH) {
        // initialize list for separator
        versionLookbacks.putIfAbsent(browser.separator, new ArrayList<>());
        // add pattern to list for separator
        versionLookbacks.get(browser.separator).add(browser.pattern);
      }
    }
    // Combine all browser patterns
    String browserPattern = String.join("|",
        Arrays.stream(Browser.values()).map(m -> m.pattern).collect(Collectors.toList()));
    // Combine all version lookback patterns
    String versionLookbackPattern = String.join("|",
        versionLookbacks.entrySet().stream()
            .map(e -> "(?<=" + String.join("|", e.getValue()) + ")" + e.getKey())
            .collect(Collectors.toList()));
    // Combine all os patterns
    String osPattern = String.join("|",
        Arrays.stream(Os.values()).map(os -> os.pattern).collect(Collectors.toList()));
    // Combine the above into a pattern that matches browser and optional version OR os
    return Pattern.compile("(?<browser>" + browserPattern + ")((" + versionLookbackPattern
        + ")(?<version>\\d+))?|(?<os>" + osPattern + ")");
  }

  private static final String LAST_SLASH = ".*/"; // Special case value for browser.separator

  private static final Map<String, Os> OS_MAP = Os.asMap();
  private static final Map<String, Browser> BROWSER_MAP = Browser.asMap();

  private static final Pattern LAST_SLASH_VERSION = Pattern.compile(".*/(?<version>\\d+)");
  private static final Pattern SEARCH = compileRegex();

  private static final Fn FN = new Fn();
  private static final ParseUserAgent INSTANCE = new ParseUserAgent();

  private static class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      // Prevent null pointer exception
      message = PubsubConstraints.ensureNonNull(message);

      // Copy attributes
      Map<String, String> attributes = new HashMap<String, String>(message.getAttributeMap());

      // Remove and record user agent if present
      String agent = attributes.remove(Attribute.USER_AGENT);

      // Parse user agent
      if (agent != null) {
        // Search agent for matches using regular expressions
        Matcher search = SEARCH.matcher(agent);
        // initialize state
        Browser browser = null;
        String version = null;
        String slashVersion = null;
        Os os = null;
        // Loop until we haven't found any matches
        while (search.find()) {
          if (search.group("browser") != null) {
            // Get Browser for detected browser
            Browser value = BROWSER_MAP.get(search.group("browser"));
            if (browser == null || value.ordinal() < browser.ordinal()) {
              // Update browser with better match
              browser = value;
              // Update version or set null
              version = search.group("version");
            } else if (value == browser && version == null) {
              // Add missing version from repeat match
              version = search.group("version");
            }
          }
          if (search.group("os") != null) {
            // Get Os for detected os
            Os value = OS_MAP.get(search.group("os"));
            if (os == null || value.ordinal() < os.ordinal()) {
              // Update os with better match
              os = value;
            }
          }
        }

        if (browser != null) {
          // put browser attribute
          attributes.put(Attribute.USER_AGENT_BROWSER, browser.value);
          if (browser.version != null) {
            // put version attribute from browser
            attributes.put(Attribute.USER_AGENT_VERSION, browser.version);
          } else if (browser.separator == LAST_SLASH) {
            // search for this pattern as late as possible because it's slow
            search = LAST_SLASH_VERSION.matcher(agent);
            if (search.find()) {
              // put version attribute from search
              attributes.put(Attribute.USER_AGENT_VERSION, search.group("version"));
            }
          } else if (version != null) {
            // put version attribute
            attributes.put(Attribute.USER_AGENT_VERSION, version);
          }
        }

        if (browser != null && browser.os != null) {
          // put os attribute from browser
          attributes.put(Attribute.USER_AGENT_OS, browser.os);
        } else if (os != null) {
          // put os attribute
          attributes.put(Attribute.USER_AGENT_OS, os.value);
        }
      }
      // Return new message
      return new PubsubMessage(message.getPayload(), attributes);
    }
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(FN));
  }

  private ParseUserAgent() {
  }

}
