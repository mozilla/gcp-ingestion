package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.io.Read.FileInput;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class ParseLogEntryTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testParse() throws Exception {
    String inputPath = Resources.getResource("testdata").getPath();
    String input = inputPath + "/logentry.ndjson";

    String expected = Json
        .asString(ImmutableMap.builder().put("ecosystem_anon_id", "fake").build());

    PCollection<PubsubMessage> output = pipeline
        .apply(new FileInput(StaticValueProvider.of(input), InputFileFormat.json))
        .apply(ParseLogEntry.of());

    PAssert.that(output.apply(OutputFileFormat.text.encode()))
        .containsInAnyOrder(ImmutableList.of(expected));

    pipeline.run();
  }

}
