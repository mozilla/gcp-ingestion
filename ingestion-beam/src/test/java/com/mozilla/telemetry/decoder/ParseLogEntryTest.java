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
public class ParseLogEntryTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testParse() throws Exception {
    String inputPath = Resources.getResource("testdata").getPath();
    String input = inputPath + "/logentry.ndjson";

    String expected = Json
        .asString(
            ImmutableMap.builder()
                .put("metrics", ImmutableMap.builder()
                    .put("string", ImmutableMap.builder().put("event.name", "reg_submit_success")
                        .put("account.user_id_sha256", "").put("relying_party.oauth_client_id", "")
                        .put("relying_party.service", "sync").put("session.device_type", "desktop")
                        .put("session.entrypoint", "")
                        .put("session.flow_id",
                            "5d1eaf933f521cb2a15af909c813673ada8485d6ace8e806c57148cd7f13b30c")
                        .put("utm.campaign", "").put("utm.content", "").put("utm.medium", "")
                        .put("utm.source", "").put("utm.term", "").build())
                    .build())
                .put("ping_info",
                    ImmutableMap.builder().put("seq", 2).put("start_time", "2023-06-22T11:28-05:00")
                        .put("end_time", "2023-06-22T11:28-05:00").build())
                .put("client_info", ImmutableMap.builder().put("telemetry_sdk_build", "1.4.0")
                    .put("client_id", "73f5ae86-90f8-46a3-9ccc-bca902a175bf")
                    .put("first_run_date", "2023-06-22-05:00").put("os", "Darwin")
                    .put("os_version", "Unknown").put("architecture", "Unknown")
                    .put("locale", "en-US").put("app_build", "Unknown")
                    .put("app_display_version", "0.0.0").put("app_channel", "development").build())
                .build());

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> outputWithFailures = pipeline
        .apply(new FileInput(input, InputFileFormat.json)).apply(ParseLogEntry.of());

    PAssert.that(outputWithFailures.output().apply(OutputFileFormat.text.encode()))
        .containsInAnyOrder(ImmutableList.of(expected));

    PAssert.that(outputWithFailures.failures()).empty();

    pipeline.run();
  }

  @Test
  public void canHandleMalformedMessage() {
    String inputPath = Resources.getResource("testdata").getPath();
    // This file contains a single message without the expected `Fields` object
    String input = inputPath + "/logentry_malformed.ndjson";

    WithFailures.Result<PCollection<PubsubMessage>, PubsubMessage> outputWithFailures = pipeline
        .apply(new FileInput(input, InputFileFormat.json)).apply(ParseLogEntry.of());

    // output should be empty
    PAssert.that(outputWithFailures.output()).empty();

    // message should go to error stream
    PAssert.that(outputWithFailures.failures() //
        .apply("EncodeTextError", OutputFileFormat.json.encode())).satisfies(collection -> {
          List<String> errors = ImmutableList.copyOf(collection);
          assert errors.size() == 1;
          assert errors.get(0).contains(
              "{\"attributeMap\":{\"error_message\":\"com.mozilla.telemetry.decoder.ParseLogEntry$InvalidLogEntryException: Message has no submission_timestamp but is not in a known LogEntry format\",\"exception_class\":\"com.mozilla.telemetry.decoder.ParseLogEntry$InvalidLogEntryException\"");
          return null;
        });

    pipeline.run();
  }
}
