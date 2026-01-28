package com.mozilla.telemetry.decoder;

import com.google.common.io.Resources;
import com.mozilla.telemetry.io.Read;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("checkstyle:lineLength")
public class ExtractIpFromDirectPubsubTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testExtractIpAndScrub() {
    String inputPath = Resources.getResource("testdata").getPath();
    String input = inputPath + "/directpubsub.ndjson";

    PCollection<PubsubMessage> output = pipeline
        .apply(new Read.FileInput(input, InputFileFormat.json))
        .apply(ExtractIpFromDirectPubsub.of());

    // validate that IP address is added as attribute
    final List<String> expectedAttributes = Arrays.asList("{\"x_forwarded_for\":\"192.168.1.1\"}");
    final PCollection<String> attributes = output
        .apply(MapElements.into(TypeDescriptors.strings()).via(m -> {
          try {
            return Json.asString(m.getAttributeMap());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }));
    PAssert.that(attributes).containsInAnyOrder(expectedAttributes);

    // validate that IP address is removed from payload
    final List<String> expectedPayloads = Arrays.asList(
        "{\"document_id\":\"6c702690-02a7-4e81-b4cb-550a1ce10a22\",\"document_namespace\":\"accounts_backend\",\"document_type\":\"events\",\"document_version\":\"1\",\"payload\":\"{\\\"metrics\\\":{\\\"string\\\":{\\\"event.name\\\":\\\"login\\\"}},\\\"ping_info\\\":{\\\"seq\\\":1}}\",\"user_agent\":\"Mozilla/5.0\"}");
    final PCollection<String> payloads = output.apply("encodeText", OutputFileFormat.text.encode());
    PAssert.that(payloads).containsInAnyOrder(expectedPayloads);

    pipeline.run();
  }

  @Test
  public void canHandleMalformedMessage() {
    String inputPath = Resources.getResource("testdata").getPath();
    // This file contains a single message without the expected required fields
    String input = inputPath + "/directpubsub_malformed.ndjson";

    PCollection<PubsubMessage> output = pipeline
        .apply(new Read.FileInput(input, InputFileFormat.json))
        .apply(ExtractIpFromDirectPubsub.of());

    // Message should pass through unchanged (ExtractIp doesn't validate structure)
    final List<String> expectedPayloads = Arrays.asList(
        "{\"document_id\":\"6c702690-02a7-4e81-b4cb-550a1ce10a22\",\"document_namespace\":\"accounts_backend\"}");
    final PCollection<String> payloads = output.apply("encodeText", OutputFileFormat.text.encode());
    PAssert.that(payloads).containsInAnyOrder(expectedPayloads);

    // Pipeline should run and not throw an exception on the malformed message
    pipeline.run();
  }
}
