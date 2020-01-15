package com.mozilla.telemetry.transforms;

import com.google.common.collect.ImmutableList;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class NormalizeAttributes
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static NormalizeAttributes of() {
    return INSTANCE;
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    return input.apply(MapElements.via(fn));
  }

  /**
   * Normalize a channel string to one of the five canonical release channels or "Other".
   *
   * <p>Reimplementation of https://github.com/mozilla-services/lua_sandbox_extensions/blob/607b3c7b9def600ddbd373a14f67a55e1c69f7a1/moz_telemetry/modules/moz_telemetry/normalize.lua#L167-L178
   */
  public static String normalizeChannel(String name) {
    if (CHANNELS.contains(name)) {
      // We could lowercase before doing this check, but don't to maintain historical practice.
      return name;
    } else if (name.startsWith("nightly-cck-")) {
      // The cck suffix was used for various deployments before Firefox Quantum;
      // cck refers to the "Client Customization Wizard", see
      // https://mike.kaply.com/2012/04/13/customizing-firefox-extensions-and-the-cck-wizard/
      return NIGHTLY;
    } else if (name.startsWith(BETA)) {
      // We have a very small tail of pings with channel set to beta with an arbitrary suffix;
      // we maintain the behavior of normalizing these to "beta" to match historical practice.
      return BETA;
    } else {
      return OTHER;
    }
  }

  /**
   * Normalize an operating system string to one of the three major desktop platforms,
   * one of the two major mobile platforms, or "Other".
   *
   * <p>Reimplements and combines the logic of {@code os} and {@code mobile_os} from
   * https://github.com/mozilla-services/lua_sandbox_extensions/blob/607b3c7b9def600ddbd373a14f67a55e1c69f7a1/moz_telemetry/modules/moz_telemetry/normalize.lua#L184-L215
   */
  public static String normalizeOs(String name) {
    if (name.startsWith("Windows") || name.startsWith("WINNT")) {
      return WINDOWS;
    } else if (name.startsWith("Darwin")) {
      return MAC;
    } else if (name.contains("Linux") || name.contains("BSD") || name.contains("SunOS")) {
      return LINUX;
    } else if (name.startsWith("iOS") || name.contains("iPhone")) {
      return IOS;
    } else if (name.startsWith("Android")) {
      return ANDROID;
    } else {
      return OTHER;
    }
  }

  /**
   * Normalize an operating system version string to at most MAJOR.MINOR.PATCH numeric version.
   *
   * <p>Strips extra text at the end of the version string, but returns an empty string if the
   * input starts with non-numeric text.
   *
   * <p>Reimplementation of https://github.com/mozilla-services/lua_sandbox_extensions/blob/607b3c7b9def600ddbd373a14f67a55e1c69f7a1/moz_telemetry/modules/moz_telemetry/normalize.lua#L196-L204
   */
  public static String normalizeOsVersion(String v) {
    Matcher matcher = OS_VERSION_RE.matcher(v);
    if (matcher.matches() && matcher.groupCount() == 1) {
      return matcher.group(1);
    } else {
      return "";
    }
  }

  /**
   * Normalize app_name as reported by the various products, or "Other" for unrecognized prefixes.
   *
   * <p>Reimplements {@code mobile_app_name} from
   * https://github.com/mozilla-services/lua_sandbox_extensions/blob/607b3c7b9def600ddbd373a14f67a55e1c69f7a1/moz_telemetry/modules/moz_telemetry/normalize.lua#L218-L230
   * and adds a case for "Firefox" desktop app.
   */
  public static String normalizeAppName(String name) {
    for (String app : APP_NAMES) {
      if (name.startsWith(app)) {
        return app;
      }
    }
    return OTHER;
  }

  /**
   * Normalize a 2-letter country code value to uppercase, otherwise return "Other".
   *
   * <p>This function is intended to be used to normalize ISO 3166-1 alpha-2 codes, but we do not
   * check that the code actually corresponds to an officially assigned country so that we don't
   * need to worry about our list of codes drifting out of date. The space of two-letter codes
   * (26 * 26 = 676) is sufficiently small that the impact of invalid codes would likely be small.
   *
   * <p>See https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
   *
   * <p>Previous implementation with a fixed list of allowable codes:
   * https://github.com/mozilla-services/lua_sandbox_extensions/blob/607b3c7b9def600ddbd373a14f67a55e1c69f7a1/moz_telemetry/modules/moz_telemetry/normalize.lua#L233-L268
   */
  public static String normalizeCountryCode(String code) {
    code = code.toUpperCase();
    Matcher matcher = COUNTRY_CODE_RE.matcher(code);
    if (matcher.matches()) {
      return code;
    } else {
      return OTHER;
    }
  }

  ////////

  private NormalizeAttributes() {
  }

  private static final NormalizeAttributes INSTANCE = new NormalizeAttributes();

  private class Fn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      message = PubsubConstraints.ensureNonNull(message);
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      Optional.ofNullable(attributes.get(Attribute.APP_UPDATE_CHANNEL))
          .map(NormalizeAttributes::normalizeChannel)
          .ifPresent(v -> attributes.put(Attribute.NORMALIZED_CHANNEL, v));
      Optional.ofNullable(attributes.get(Attribute.OS)) //
          .map(NormalizeAttributes::normalizeOs) //
          .ifPresent(v -> attributes.put(Attribute.NORMALIZED_OS, v));
      Optional.ofNullable(attributes.get(Attribute.OS_VERSION))
          .map(NormalizeAttributes::normalizeOsVersion)
          .ifPresent(v -> attributes.put(Attribute.NORMALIZED_OS_VERSION, v));
      Optional.ofNullable(attributes.get(Attribute.APP_NAME))
          .map(NormalizeAttributes::normalizeAppName)
          .ifPresent(v -> attributes.put(Attribute.NORMALIZED_APP_NAME, v));
      Optional.ofNullable(attributes.get(Attribute.GEO_COUNTRY))
          .map(NormalizeAttributes::normalizeCountryCode)
          .ifPresent(v -> attributes.put(Attribute.NORMALIZED_COUNTRY_CODE, v));

      return new PubsubMessage(message.getPayload(), attributes, message.getMessageId());
    }
  }

  private final Fn fn = new Fn();

  private static final String OTHER = "Other";

  private static final String BETA = "beta";
  private static final String NIGHTLY = "nightly";
  private static final List<String> CHANNELS = ImmutableList.of("release", "esr", BETA, "aurora",
      NIGHTLY);

  private static final String WINDOWS = "Windows";
  private static final String MAC = "Mac";
  private static final String LINUX = "Linux";
  private static final String IOS = "iOS";
  private static final String ANDROID = "Android";

  private static final List<String> APP_NAMES = ImmutableList.of(
      // Make sure more-specific versions come before Zerda.
      "Zerda_cn", "Zerda",
      // Make sure more-specific versions come before Focus.
      "Focus-TV", "Focus", "Klar",
      // FirefoxRealityX
      "FirefoxReality_oculusvr", "FirefoxReality_googlevr", "FirefoxReality_wavevr",
      // FirefoxX
      "FirefoxForFireTV", "FirefoxConnect",
      // Other apps
      "Fennec", "Scryer", "WebXR", "Lockbox",
      // Desktop Firefox
      "Firefox");

  private static final Pattern OS_VERSION_RE = Pattern.compile("([0-9]+(?:\\.[0-9]+){0,2}).*");
  private static final Pattern COUNTRY_CODE_RE = Pattern.compile("[A-Z][A-Z]");

}
