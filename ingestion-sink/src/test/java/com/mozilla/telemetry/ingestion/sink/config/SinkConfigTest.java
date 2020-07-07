package com.mozilla.telemetry.ingestion.sink.config;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.collect.ImmutableSet;
import com.mozilla.telemetry.ingestion.sink.util.Env;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SinkConfigTest {

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void canDetectBqProject() {
    environmentVariables.set("GOOGLE_CLOUD_PROJECT", "gcp-project");
    environmentVariables.set("BIG_QUERY_DEFAULT_PROJECT", "bq-project");
    Env env = new Env(ImmutableSet.of("BIG_QUERY_DEFAULT_PROJECT"));
    assertEquals("gcp-project", BigQueryOptions.getDefaultInstance().getProjectId());
    assertEquals("bq-project", SinkConfig.getBigQueryService(env).getOptions().getProjectId());
  }
}
