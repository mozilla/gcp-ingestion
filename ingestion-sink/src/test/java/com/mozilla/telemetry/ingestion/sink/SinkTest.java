package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.mozilla.telemetry.ingestion.core.schema.TestConstant;
import com.mozilla.telemetry.ingestion.sink.config.SinkConfig;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.contrib.java.lang.system.TextFromStandardInputStream;

public class SinkTest {

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Rule
  public final TextFromStandardInputStream systemIn = TextFromStandardInputStream
      .emptyStandardInputStream();

  @Rule
  public final SystemOutRule systemOut = new SystemOutRule();

  @Rule
  public final SystemErrRule systemErr = new SystemErrRule();

  @Test
  public void failsOnMissingInput() {
    environmentVariables.set("OUTPUT_TABLE", "dataset.table");
    assertThrows(IllegalArgumentException.class, () -> Sink.main(null));
  }

  @Test
  public void failsOnInvalidErrorOutput() {
    environmentVariables.set("INPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_MAX_ATTEMPTS", "1");
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        SinkConfig::getOutput);
    assertEquals("Invalid error output type detected: bigQueryLoad", thrown.getMessage());
  }

  @Test
  public void canDeliverErrors() throws IOException {
    environmentVariables.set("INPUT_PIPE", "stdin");
    environmentVariables.set("OUTPUT_PIPE", "stdout");
    environmentVariables.set("OUTPUT_FORMAT", "payload");
    environmentVariables.set("SCHEMAS_LOCATION", TestConstant.SCHEMAS_LOCATION);
    environmentVariables.set("ERROR_OUTPUT_PIPE", "stderr");
    // one message with no valid payload format, but a valid default format of
    systemIn.provideLines("{}");
    systemOut.enableLog();
    systemOut.mute();
    systemErr.enableLog();
    systemErr.mute();
    Sink.main(null);
    assertEquals("", systemOut.getLog());
    assertEquals("{}\n", systemErr.getLog());
  }
}
