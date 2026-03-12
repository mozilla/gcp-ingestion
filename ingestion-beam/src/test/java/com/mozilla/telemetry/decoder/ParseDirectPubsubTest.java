package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.io.Read.FileInput;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.Json;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("checkstyle:lineLength")
public class ParseDirectPubsubTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testParse() throws Exception {
    String inputPath = Resources.getResource("testdata").getPath();
    String input = inputPath + "/directpubsub.ndjson";

    String expected = Json
        .asString(ImmutableMap.builder()
            .put("metrics", ImmutableMap.builder()
                .put("string", ImmutableMap.builder().put("event.name", "login").build()).build())
            .put("ping_info", ImmutableMap.builder().put("seq", 1).build()).build());

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> outputWithFailures = pipeline
        .apply(new FileInput(input, InputFileFormat.json)).apply(ParseDirectPubsub.of());

    // Verify payload is extracted correctly
    PAssert.that(outputWithFailures.output().apply(OutputFileFormat.text.encode()))
        .containsInAnyOrder(ImmutableList.of(expected));

    // Verify submission_timestamp attribute is set from element timestamp
    PAssert.that(outputWithFailures.output()).satisfies(collection -> {
      List<PubsubMessage> messages = ImmutableList.copyOf(collection);
      assert messages.size() == 1;
      PubsubMessage message = messages.get(0);
      assert message
          .getAttribute("submission_timestamp") != null : "submission_timestamp must be set";
      assert message.getAttribute("document_id").equals("6c702690-02a7-4e81-b4cb-550a1ce10a22");
      assert message.getAttribute("document_namespace").equals("accounts_backend");
      assert message.getAttribute("document_type").equals("events");
      assert message.getAttribute("document_version").equals("1");
      return null;
    });

    PAssert.that(outputWithFailures.failures()).empty();

    pipeline.run();
  }

  @Test
  public void canHandleMalformedMessage() {
    String inputPath = Resources.getResource("testdata").getPath();
    // This file contains a message missing required fields
    String input = inputPath + "/directpubsub_malformed.ndjson";

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> outputWithFailures = pipeline
        .apply(new FileInput(input, InputFileFormat.json)).apply(ParseDirectPubsub.of());

    // output should be empty
    PAssert.that(outputWithFailures.output()).empty();

    // message should go to error stream
    PAssert.that(outputWithFailures.failures() //
        .apply("EncodeTextError", OutputFileFormat.json.encode())).satisfies(collection -> {
          List<String> errors = ImmutableList.copyOf(collection);
          assert errors.size() == 1;
          assert errors.get(0).contains(
              "{\"attributeMap\":{\"error_message\":\"com.mozilla.telemetry.decoder.ParseDirectPubsub$InvalidDirectPubsubException: Message is not in valid Direct Pub/Sub submission format\",\"exception_class\":\"com.mozilla.telemetry.decoder.ParseDirectPubsub$InvalidDirectPubsubException\"");
          return null;
        });

    pipeline.run();
  }
}
