package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Base64;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.contrib.java.lang.system.TextFromStandardInputStream;
import org.junit.rules.TemporaryFolder;

public class SinkPipeTest {

  @Rule
  public final TextFromStandardInputStream systemIn = TextFromStandardInputStream
      .emptyStandardInputStream();

  @Rule
  public final SystemOutRule systemOut = new SystemOutRule();

  @Rule
  public final SystemErrRule systemErr = new SystemErrRule();

  @Rule
  public TemporaryFolder tempdir = new TemporaryFolder();

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  private String asBase64(String data) {
    return Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void canFormatAsBeam() throws IOException {
    environmentVariables.set("INPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_FORMAT", "beam");
    systemIn.provideLines("{\"attributeMap\":{\"meta\":\"data\"},\"payload\":\"dGVzdA==\"}",
        "{\"payload\":{\"meta\":\"data\"}}", "{\"payload\":null}", "{}");
    systemOut.enableLog();
    systemOut.mute();
    Sink.main(null);
    assertEquals("{\"attributeMap\":{\"meta\":\"data\"},\"payload\":\"dGVzdA==\"}\n" //
        + "{\"payload\":\"" + asBase64("{\"meta\":\"data\"}") + "\"}\n" //
        + "{}\n" + "{}\n", //
        systemOut.getLog());
  }

  @Test
  public void canWriteStderr() throws IOException {
    environmentVariables.set("INPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_PIPE", "2");
    systemIn.provideLines("{}");
    systemErr.enableLog();
    systemErr.mute();
    Sink.main(null);
    assertEquals("{}\n", systemErr.getLog());
  }

  @Test
  public void canReadAndWriteFiles() throws IOException {
    final File input = tempdir.newFile("input.ndjson");
    environmentVariables.set("INPUT_PIPE", input.getPath());
    final File output = tempdir.newFile("output.ndjson");
    environmentVariables.set("OUTPUT_PIPE", output.getPath());
    try (PrintStream inputStream = new PrintStream(input)) {
      inputStream.println("{}");
    }
    Sink.main(null);
    assertEquals(ImmutableList.of("{}"), Files.readAllLines(output.toPath()));
  }

  @Test
  public void throwsOnMissingOutputDir() {
    environmentVariables.set("INPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_PIPE",
        tempdir.getRoot().toPath().resolve("missing dir").resolve("output.ndjson").toString());
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> Sink.main(null));
    assertTrue(thrown.getCause() instanceof FileNotFoundException);
  }

  @Test
  public void throwsOnMissingInputDir() {
    environmentVariables.set("INPUT_PIPE",
        tempdir.getRoot().toPath().resolve("input.ndjson").toString());
    environmentVariables.set("OUTPUT_PIPE", "-");
    assertThrows(NoSuchFileException.class, () -> Sink.main(null));
  }

  @Test
  public void throwsOnInvalidTableId() {
    environmentVariables.set("INPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_TABLE", "table_without_dataset");
    systemIn.provideLines("{}");
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> Sink.main(null));
    assertEquals("TableId requires dataset but none found in: table_without_dataset",
        thrown.getMessage());
  }

  @Test
  public void throwsOnInvalidInput() {
    environmentVariables.set("INPUT_PIPE", "-");
    environmentVariables.set("OUTPUT_PIPE", "-");
    systemIn.provideLines("}");
    UncheckedIOException thrown = assertThrows(UncheckedIOException.class, () -> Sink.main(null));
    assertTrue(thrown.getCause() instanceof JsonParseException);
  }
}
