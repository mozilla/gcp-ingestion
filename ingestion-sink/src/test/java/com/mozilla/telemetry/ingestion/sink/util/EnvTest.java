package com.mozilla.telemetry.ingestion.sink.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class EnvTest {

  @Rule
  public final EnvironmentVariables envVars = new EnvironmentVariables();

  @Test
  public void canRemovePrefix() {
    envVars.set("OUTPUT", "o");
    envVars.set("ERROR_OUTPUT", "eo");
    assertEquals("eo", new Env(ImmutableSet.of("OUTPUT"), "ERROR_").getString("OUTPUT"));
  }

  @Test
  public void canRestorePrefix() {
    envVars.set("OUTPUT", "o");
    envVars.set("ERROR_OUTPUT", "eo");
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> new Env(ImmutableSet.of("OUTPUT"), "ERROR_").requireAllVarsUsed());
    assertEquals("Env vars set but not used: [ERROR_OUTPUT]", thrown.getMessage());
  }

  @Test
  public void throwsMissingInclude() {
    envVars.set("OUTPUT", "o");
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> new Env(ImmutableSet.of()).getString("OUTPUT"));
    assertEquals("key missing from include: OUTPUT", thrown.getMessage());
  }

  @Test
  public void throwsMissingRequired() {
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> new Env(ImmutableSet.of("OUTPUT")).getString("OUTPUT"));
    assertEquals("Missing required env var: OUTPUT", thrown.getMessage());
  }
}
