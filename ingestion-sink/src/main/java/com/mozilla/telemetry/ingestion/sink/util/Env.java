package com.mozilla.telemetry.ingestion.sink.util;

import com.mozilla.telemetry.ingestion.core.util.Time;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Env {

  private final Map<String, String> env;
  private final Set<String> unused;

  /** Constructor. */
  public Env(List<String> include) {
    env = include.stream().filter(key -> System.getenv(key) != null)
        .collect(Collectors.toMap(key -> key, System::getenv));
    unused = new HashSet<>(env.keySet());
  }

  /**
   * Throw an {@link IllegalArgumentException} for any environment variables set but not used.
   */
  public void requireAllVarsUsed() {
    if (!unused.isEmpty()) {
      throw new IllegalArgumentException("Env vars set but not used: " + unused.toString());
    }
  }

  public boolean containsKey(String key) {
    return env.containsKey(key);
  }

  public Optional<String> optString(String key) {
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

  public Duration getDuration(String key, String defaultValue) {
    return Time.parseJavaDuration(getString(key, defaultValue));
  }
}
