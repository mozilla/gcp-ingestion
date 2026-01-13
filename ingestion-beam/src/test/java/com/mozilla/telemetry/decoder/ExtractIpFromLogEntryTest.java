package com.mozilla.telemetry.decoder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
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
public class ExtractIpFromLogEntryTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testExtractIpAndScrub() throws IOException {
    String inputPath = Resources.getResource("testdata").getPath();
    String input = inputPath + "/logentry.ndjson";

    PCollection<PubsubMessage> output = pipeline
        .apply(new Read.FileInput(input, InputFileFormat.json)).apply(ExtractIpFromLogEntry.of());

    // validate that IP address is added as attribute
    final List<String> expectedAttributes = Arrays.asList(
        "{\"logging.googleapis.com/timestamp\":\"2023-06-28T07:29:17.075926141Z\",\"x_forwarded_for\":\"2a02:a311:803c:6300:4074:5cf2:91ac:d546\"}");
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
    final JsonNode expectedPayload = Json.readObjectNode(
        "{\"insertId\":\"i7ymfvfsay14vgp7\",\"jsonPayload\":{\"EnvVersion\":\"2.0\",\"Fields\":{\"document_id\":\"dd99db96-941d-4894-bb97-7a2bcd65bbf5\",\"document_namespace\":\"accounts-backend\",\"document_type\":\"accounts-events\",\"document_version\":\"1\",\"user_agent\":\"Mozilla/5.0 (X11; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0\",\"payload\":\"{\\\"metrics\\\":{\\\"string\\\":{\\\"event.name\\\":\\\"reg_submit_success\\\",\\\"account.user_id_sha256\\\":\\\"\\\",\\\"relying_party.oauth_client_id\\\":\\\"\\\",\\\"relying_party.service\\\":\\\"sync\\\",\\\"session.device_type\\\":\\\"desktop\\\",\\\"session.entrypoint\\\":\\\"\\\",\\\"session.flow_id\\\":\\\"5d1eaf933f521cb2a15af909c813673ada8485d6ace8e806c57148cd7f13b30c\\\",\\\"utm.campaign\\\":\\\"\\\",\\\"utm.content\\\":\\\"\\\",\\\"utm.medium\\\":\\\"\\\",\\\"utm.source\\\":\\\"\\\",\\\"utm.term\\\":\\\"\\\"}},\\\"ping_info\\\":{\\\"seq\\\":2,\\\"start_time\\\":\\\"2023-06-22T11:28-05:00\\\",\\\"end_time\\\":\\\"2023-06-22T11:28-05:00\\\"},\\\"client_info\\\":{\\\"telemetry_sdk_build\\\":\\\"1.4.0\\\",\\\"client_id\\\":\\\"73f5ae86-90f8-46a3-9ccc-bca902a175bf\\\",\\\"first_run_date\\\":\\\"2023-06-22-05:00\\\",\\\"os\\\":\\\"Darwin\\\",\\\"os_version\\\":\\\"Unknown\\\",\\\"architecture\\\":\\\"Unknown\\\",\\\"locale\\\":\\\"en-US\\\",\\\"app_build\\\":\\\"Unknown\\\",\\\"app_display_version\\\":\\\"0.0.0\\\",\\\"app_channel\\\":\\\"development\\\"}}\"},\"Logger\":\"fxa-oauth-server\",\"Pid\":26,\"Severity\":6,\"Timestamp\":1.68793735707500006E18,\"Type\":\"glean-server-event\"},\"labels\":{\"compute.googleapis.com/resource_name\":\"gke-custom-fluentbit-default-pool-3fa9e570-j753\",\"k8s-pod/component\":\"test-js-logger\",\"k8s-pod/pod-template-hash\":\"584d9fc78c\"},\"logName\":\"projects/akomar-server-telemetry-poc/logs/stdout\",\"receiveTimestamp\":\"2023-06-28T07:29:17.288291899Z\",\"resource\":{\"labels\":{\"cluster_name\":\"custom-fluentbit\",\"container_name\":\"test-js-logger\",\"location\":\"us-east1-b\",\"namespace_name\":\"default\",\"pod_name\":\"test-js-logger-584d9fc78c-pz9x9\",\"project_id\":\"akomar-server-telemetry-poc\"},\"type\":\"k8s_container\"},\"severity\":\"INFO\",\"timestamp\":\"2023-06-28T07:29:17.075926141Z\"}");
    PAssert.thatSingleton(output).satisfies(message -> {
      try {
        JsonNode actualPayload = Json.readObjectNode(message.getPayload());
        // JsonNode.equals() uses deep value equality, avoiding platform-specific float formatting
        assertTrue("Payload should match expected JSON", actualPayload.equals(expectedPayload));
        // Verify ip_address was removed from Fields
        assertFalse("ip_address should be removed from Fields",
            actualPayload.path("jsonPayload").path("Fields").has("ip_address"));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return null;
    });

    pipeline.run();
  }

  @Test
  public void canHandleMalformedMessage() {
    String inputPath = Resources.getResource("testdata").getPath();
    // This file contains a single message without the expected `Fields` object
    String input = inputPath + "/logentry_malformed.ndjson";

    PCollection<PubsubMessage> output = pipeline
        .apply(new Read.FileInput(input, InputFileFormat.json)).apply(ExtractIpFromLogEntry.of());

    final List<String> expectedPayloads = Arrays.asList(
        "{\"insertId\":\"i7ymfvfsay14vgp7\",\"jsonPayload\":{\"EnvVersion\":\"2.0\",\"Logger\":\"fxa-oauth-server\",\"Pid\":26,\"Severity\":6,\"Timestamp\":1.6879373570750001e+18,\"Type\":\"glean-server-event\"},\"labels\":{\"compute.googleapis.com/resource_name\":\"gke-custom-fluentbit-default-pool-3fa9e570-j753\",\"k8s-pod/component\":\"test-js-logger\",\"k8s-pod/pod-template-hash\":\"584d9fc78c\"},\"logName\":\"projects/akomar-server-telemetry-poc/logs/stdout\",\"receiveTimestamp\":\"2023-06-28T07:29:17.288291899Z\",\"resource\":{\"labels\":{\"cluster_name\":\"custom-fluentbit\",\"container_name\":\"test-js-logger\",\"location\":\"us-east1-b\",\"namespace_name\":\"default\",\"pod_name\":\"test-js-logger-584d9fc78c-pz9x9\",\"project_id\":\"akomar-server-telemetry-poc\"},\"type\":\"k8s_container\"},\"severity\":\"INFO\",\"timestamp\":\"2023-06-28T07:29:17.075926141Z\"}");
    final PCollection<String> payloads = output.apply("encodeText", OutputFileFormat.text.encode());
    PAssert.that(payloads).containsInAnyOrder(expectedPayloads);

    // Pipeline should run and not throw an exception on the malformed message
    pipeline.run();
  }

  @Test
  public void canHandleNullIpAddress() {
    String inputPath = Resources.getResource("testdata").getPath();
    // This file contains a message with ip_address: null (JSON null)
    String input = inputPath + "/logentry_null_ip.ndjson";

    PCollection<PubsubMessage> output = pipeline
        .apply(new Read.FileInput(input, InputFileFormat.json)).apply(ExtractIpFromLogEntry.of());

    // Validate that x_forwarded_for is NOT added when ip_address is null
    final PCollection<String> attributes = output
        .apply(MapElements.into(TypeDescriptors.strings()).via(m -> {
          try {
            return Json.asString(m.getAttributeMap());
          } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
          }
        }));
    final List<String> expectedAttributes = Arrays
        .asList("{\"logging.googleapis.com/timestamp\":\"2023-06-28T07:29:17.075926141Z\"}");
    PAssert.that(attributes).containsInAnyOrder(expectedAttributes);

    pipeline.run();
  }
}
