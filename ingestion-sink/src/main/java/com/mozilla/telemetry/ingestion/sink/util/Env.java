package com.mozilla.telemetry.ingestion.sink.util;

import com.mozilla.telemetry.ingestion.core.util.Time;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Env {

  private final Set<String> include;
  private final String prefix;
  private final Map<String, String> env;
  private final Set<String> unused;

  /** Constructor with default empty prefix. */
  public Env(Set<String> include) {
    this(include, "");
  }

  /** Add prefix to env vars and access values without the prefix.
   *
   * <p>This is used to allow identical options for output and error output.
   */
  public Env(Set<String> include, String prefix) {
    this.include = include;
    this.prefix = prefix;
    env = include.stream().filter(key -> System.getenv(prefix + key) != null)
        .collect(Collectors.toMap(key -> key, key -> System.getenv(prefix + key)));
    unused = new HashSet<>(env.keySet());
  }

  /**
   * Throw an {@link IllegalArgumentException} for any environment variables set but not used.
   */
  public void requireAllVarsUsed() {
    if (!unused.isEmpty()) {
      throw new IllegalArgumentException("Env vars set but not used: "
          // add prefix back in to get actual env vars
          + unused.stream().map(key -> prefix + key).collect(Collectors.toList()).toString());
    }
  }

  public boolean isEmpty() {
    return env.isEmpty();
  }

  public boolean containsKey(String key) {
    return env.containsKey(key);
  }

  /** Get the value of an optional environment variable. */
  public Optional<String> optString(String key) {
    if (!include.contains(key)) {
      throw new IllegalArgumentException("key missing from include: " + key);
    }
    unused.remove(key);
    return Optional.ofNullable(env.get(key));
  }

  public String getString(String key) {
    return optString(key)
        .orElseThrow(() -> new IllegalArgumentException("Missing required env var: " + key));
  }

  public String getString(String key, String defaultValue) {
    return optString(key).orElse(defaultValue);
  }

  public List<String> getStrings(String key, List<String> defaultValue) {
    return optString(key).filter(s -> !s.isEmpty()).map(s -> Arrays.asList(s.split(",")))
        .orElse(defaultValue);
  }

  public Integer getInt(String key, Integer defaultValue) {
    return optString(key).map(Integer::new).orElse(defaultValue);
  }

  public Long getLong(String key, Long defaultValue) {
    return optString(key).map(Long::new).orElse(defaultValue);
  }

  public Boolean getBool(String key, Boolean defaultValue) {
    return optString(key).map(Boolean::valueOf).orElse(defaultValue);
  }

  public Duration getDuration(String key, String defaultValue) {
    return Time.parseJavaDuration(getString(key, defaultValue));
  }

  public Pattern getPattern(String key) {
    return Pattern.compile(getString(key));
  }
}
