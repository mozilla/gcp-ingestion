package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.transforms.WithErrors;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class ParseUriTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    final List<String> validInput = Arrays.asList(//
        "{\"attributeMap\":{},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"uri\":\"/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648"
            + "/main/Firefox/61.0a1/nightly/20180328030202\"" //
            + "},\"payload\":\"\"}",
        "{\"attributeMap\":"
            + "{\"uri\":\"/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049\""
            + "},\"payload\":\"\"}");

    final List<String> invalidInput = Arrays.asList(//
        "{\"attributeMap\":" //
            + "{\"uri\":\"/nonexistent_prefix/ce39b608-f595-4c69-b6a6-f7a436604648" //
            + "/main/Firefox/61.0a1/nightly/20180328030202\"" //
            + "},\"payload\":\"\"}",
        "{\"attributeMap\":" //
            + "{\"uri\":\"/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648"
            + "/Firefox/61.0a1/nightly/20180328030202\"" //
            + "},\"payload\":\"\"}",
        "{\"attributeMap\":" //
            + "{\"uri\":\"/submit/eng-workflow/hgpush/2c3a0767-d84a-4d02-8a92-fa54a3376049\"" //
            + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"app_build_id\":\"20180328030202\"" //
            + ",\"app_name\":\"Firefox\"" //
            + ",\"app_update_channel\":\"nightly\"" //
            + ",\"app_version\":\"61.0a1\"" //
            + ",\"document_id\":\"ce39b608-f595-4c69-b6a6-f7a436604648\"" //
            + ",\"document_namespace\":\"telemetry\"" //
            + ",\"document_type\":\"main\"" //
            + ",\"uri\":\"/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648" //
            + "/main/Firefox/61.0a1/nightly/20180328030202\"" //
            + "},\"payload\":\"\"}",
        "{\"attributeMap\":" //
            + "{\"document_id\":\"2c3a0767-d84a-4d02-8a92-fa54a3376049\"" //
            + ",\"document_namespace\":\"eng-workflow\"" //
            + ",\"document_type\":\"hgpush\"" //
            + ",\"document_version\":\"1\"" //
            + ",\"uri\":\"/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049\"" //
            + "},\"payload\":\"\"}");

    WithErrors.Result<PCollection<PubsubMessage>> parsed = pipeline
        .apply(Create.of(Iterables.concat(validInput, invalidInput)))
        .apply("DecodeJsonInput", InputFileFormat.json.decode()).output() //
        .apply(ParseUri.of());

    PCollection<String> output = parsed.output() //
        .apply("EncodeJsonOutput", OutputFileFormat.json.encode());
    PAssert.that(output).containsInAnyOrder(expected);

    PCollection<String> exceptions = parsed.errors() //
        .apply(MapElements.into(TypeDescriptors.strings())
            .via(message -> message.getAttribute("exception_class")));
    PAssert.that(exceptions).containsInAnyOrder(
        Arrays.asList("com.mozilla.telemetry.decoder.ParseUri$InvalidUriException",
            "com.mozilla.telemetry.decoder.ParseUri$UnexpectedPathElementsException",
            "com.mozilla.telemetry.decoder.ParseUri$UnexpectedPathElementsException"));

    pipeline.run();
  }

  private static Map<String, String> validStringCases(String key) {
    return ImmutableMap.of(//
        "string", "\"" + key + "\":\"string\"", //
        key, "\"" + key + "\":\"" + key + "\"");
  }

  private static Map<String, String> validBoolCases(String key) {
    return ImmutableMap.of(//
        " ", "\"" + key + "\":false", //
        "x", "\"" + key + "\":false", //
        "0", "\"" + key + "\":false", //
        "1", "\"" + key + "\":true");
  }

  private static Map<String, String> validIntCases(String key) {
    return ImmutableMap.of(//
        "0", "\"" + key + "\":0", //
        "1", "\"" + key + "\":1", //
        "100", "\"" + key + "\":100");
  }

  private static Map<String, String> validIntAsStringCases(String key) {
    return ImmutableMap.of(//
        "0", "\"" + key + "\":\"0\"", //
        "1", "\"" + key + "\":\"1\"", //
        "100", "\"" + key + "\":\"100\"");
  }

  private static Map<String, String> joinCases(List<Map<String, String>> cases) {
    if (cases.size() > 1) {
      List<Map.Entry<String, String>> ours = new ArrayList<>(cases.get(0).entrySet());
      List<Map.Entry<String, String>> theirs = new ArrayList<>(
          joinCases(cases.subList(1, cases.size())).entrySet());
      int newSize = Math.max(ours.size(), theirs.size());
      ImmutableMap.Builder<String, String> result = new ImmutableMap.Builder<>();
      for (int i = 0; i < newSize; i++) {
        Map.Entry<String, String> ourEntry = ours.get(i % ours.size());
        Map.Entry<String, String> theirEntry = theirs.get(i % theirs.size());
        result.put(ourEntry.getKey() + "/" + theirEntry.getKey(),
            Optional.of(ourEntry.getValue()).filter(v -> !v.isEmpty()).map(v -> v + ",").orElse("")
                + theirEntry.getValue());
      }
      return result.build();
    } else {
      return cases.get(0);
    }
  }

  private static final Map<String, String> VALID_V6_DIMENSIONS = joinCases(
      Arrays.asList(validStringCases("build_channel"), //
          validStringCases("update_channel"), //
          validStringCases("locale"), //
          // Build architecture code
          ImmutableMap.of(//
              "0", "\"64bit_build\":false", //
              "1", "\"64bit_build\":true"),
          // OS architecture code
          ImmutableMap.of(//
              "0", "\"64bit_os\":false", //
              "1", "\"64bit_os\":true"), //
          ImmutableMap.of("1/2/3", "\"os_version\":\"1.2.3\""), //
          validStringCases("service_pack"), //
          validBoolCases("server_os"), //
          // Exit code
          new ImmutableMap.Builder<String, String>() //
              .put("0",
                  "\"succeeded\":true,\"user_cancelled\":false,\"out_of_retries\":false,"
                      + "\"file_error\":false,\"sig_not_trusted\":false,\"sig_unexpected\":false,"
                      + "\"install_timeout\":false")
              .put("1",
                  "\"succeeded\":false,\"user_cancelled\":false,\"out_of_retries\":false,"
                      + "\"file_error\":false,\"sig_not_trusted\":false,\"sig_unexpected\":false,"
                      + "\"install_timeout\":false")
              .put("10",
                  "\"succeeded\":false,\"user_cancelled\":true,\"out_of_retries\":false,"
                      + "\"file_error\":false,\"sig_not_trusted\":false,\"sig_unexpected\":false,"
                      + "\"install_timeout\":false")
              .put("11",
                  "\"succeeded\":false,\"user_cancelled\":false,\"out_of_retries\":true,"
                      + "\"file_error\":false,\"sig_not_trusted\":false,\"sig_unexpected\":false,"
                      + "\"install_timeout\":false")
              .put("20",
                  "\"succeeded\":false,\"user_cancelled\":false,\"out_of_retries\":false,"
                      + "\"file_error\":true,\"sig_not_trusted\":false,\"sig_unexpected\":false,"
                      + "\"install_timeout\":false")
              .put("21",
                  "\"succeeded\":false,\"user_cancelled\":false,\"out_of_retries\":false,"
                      + "\"file_error\":false,\"sig_not_trusted\":true,\"sig_unexpected\":false,"
                      + "\"install_timeout\":false")
              .put("22",
                  "\"succeeded\":false,\"user_cancelled\":false,\"out_of_retries\":false,"
                      + "\"file_error\":false,\"sig_not_trusted\":false,\"sig_unexpected\":true,"
                      + "\"install_timeout\":false")
              .put("23",
                  "\"succeeded\":false,\"user_cancelled\":false,\"out_of_retries\":false,"
                      + "\"file_error\":false,\"sig_not_trusted\":true,\"sig_unexpected\":true,"
                      + "\"install_timeout\":false")
              .put("30",
                  "\"succeeded\":false,\"user_cancelled\":false,\"out_of_retries\":false,"
                      + "\"file_error\":false,\"sig_not_trusted\":false,\"sig_unexpected\":false,"
                      + "\"install_timeout\":true")
              .build(), //
          // Launch code
          ImmutableMap.of(//
              "0", "\"old_running\":false,\"new_launched\":false", //
              "1", "\"old_running\":true,\"new_launched\":false", //
              "2", "\"old_running\":false,\"new_launched\":true"), //
          validIntCases("download_retries"), //
          validIntCases("bytes_downloaded"), //
          validIntCases("download_size"), //
          validIntCases("intro_time"), //
          validIntCases("options_time"), //
          validIntCases("download_time"), //
          ImmutableMap.of("", ""), // ignored field
          validIntCases("download_latency"), //
          validIntCases("preinstall_time"), //
          validIntCases("install_time"), //
          validIntCases("finish_time"), //
          // Initial install requirements code
          ImmutableMap.of(//
              "0", "\"disk_space_error\":false,\"no_write_access\":false", //
              "1", "\"disk_space_error\":true,\"no_write_access\":false", //
              "2", "\"disk_space_error\":false,\"no_write_access\":true"), //
          validBoolCases("manual_download"), //
          validBoolCases("had_old_install"), //
          validStringCases("old_version"), //
          validStringCases("old_build_id"), //
          validStringCases("version"), //
          validStringCases("build_id"), //
          validBoolCases("default_path"), //
          validBoolCases("admin_user"), //
          // Default browser status code
          ImmutableMap.of(//
              "0", "\"new_default\":false,\"old_default\":false", //
              "1", "\"new_default\":true,\"old_default\":false", //
              "2", "\"new_default\":false,\"old_default\":true"), //
          // Default browser setting code
          ImmutableMap.of(//
              "0", "\"set_default\":false", //
              "1", "\"set_default\":false", //
              "2", "\"set_default\":true"), //
          validStringCases("download_ip")));

  private static final Map<String, String> VALID_V7_DIMENSIONS = joinCases(
      Arrays.asList(VALID_V6_DIMENSIONS, validStringCases("attribution")));

  private static final Map<String, String> VALID_V8_DIMENSIONS = joinCases(
      Arrays.asList(VALID_V7_DIMENSIONS, //
          validIntAsStringCases("profile_cleanup_prompt"), //
          validBoolCases("profile_cleanup_requested")));

  private static final Map<String, String> VALID_V6_CASES = joinCases(Arrays.asList(//
      ImmutableMap.of("v6", "\"ping_version\":\"v6\"", //
          "v6-1", "\"ping_version\":\"v6-1\",\"funnelcake\":\"1\""), //
      VALID_V6_DIMENSIONS));

  private static final Map<String, String> VALID_V7_CASES = joinCases(Arrays.asList(//
      ImmutableMap.of("v7", "\"ping_version\":\"v7\"", //
          "v7-2", "\"ping_version\":\"v7-2\",\"funnelcake\":\"2\""), //
      VALID_V7_DIMENSIONS));

  private static final Map<String, String> VALID_V8_CASES = joinCases(Arrays.asList(//
      ImmutableMap.of("v8", "\"ping_version\":\"v8\"", //
          "v8-3", "\"ping_version\":\"v8-3\",\"funnelcake\":\"3\""), //
      VALID_V8_DIMENSIONS));

  @Test
  public void testStubUri() {
    final List<String> input = Streams.stream(Iterables.concat(//
        VALID_V6_CASES.keySet(), //
        VALID_V7_CASES.keySet(), //
        VALID_V8_CASES.keySet(), //
        Arrays.asList(//
            "v6/" + String.join("/", Collections.nCopies(35, " ")), //
            "v7/" + String.join("/", Collections.nCopies(36, " ")), //
            "v8/" + String.join("/", Collections.nCopies(38, " ")), //
            "", "v6", "v7", "v8")))
        .map(v -> "{\"attributeMap\":{\"uri\":\"/stub/" + v + "\"},\"payload\":\"\"}")
        .collect(Collectors.toList());

    final List<String> expectedPayloads = Streams.stream(Iterables.concat(//
        VALID_V6_CASES.values(), //
        VALID_V7_CASES.values(), //
        VALID_V8_CASES.values())) //
        .map(v -> sortJSON("{\"installer_type\":\"stub\",\"installer_version\":\"\"," + v + "}"))
        .collect(Collectors.toList());
    final List<String> expectedAttributes = Collections.nCopies(expectedPayloads.size(),
        "{\"document_namespace\":\"firefox-installer\","
            + "\"document_type\":\"install\",\"document_version\":\"1\"}");
    final List<String> expectedExceptions = Arrays.asList(
        "com.mozilla.telemetry.decoder.ParseUri$StubUri$InvalidIntegerException",
        "com.mozilla.telemetry.decoder.ParseUri$StubUri$InvalidIntegerException",
        "com.mozilla.telemetry.decoder.ParseUri$StubUri$InvalidIntegerException",
        "com.mozilla.telemetry.decoder.ParseUri$StubUri$UnknownPingVersionException",
        "com.mozilla.telemetry.decoder.ParseUri$UnexpectedPathElementsException",
        "com.mozilla.telemetry.decoder.ParseUri$UnexpectedPathElementsException",
        "com.mozilla.telemetry.decoder.ParseUri$UnexpectedPathElementsException");

    WithErrors.Result<PCollection<PubsubMessage>> parsed = pipeline.apply(Create.of(input))
        .apply("DecodeJsonInput", InputFileFormat.json.decode()).output() //
        .apply(ParseUri.of());

    PCollection<PubsubMessage> output = parsed.output();

    PCollection<String> payloads = output //
        .apply("PayloadString", MapElements.into(TypeDescriptors.strings())
            .via(message -> sortJSON(new String(message.getPayload(), StandardCharsets.UTF_8))));
    PAssert.that(payloads).containsInAnyOrder(expectedPayloads);

    PCollection<String> attributesSansUri = output //
        .apply("AttributesSansUri", MapElements.into(TypeDescriptors.strings()).via(message -> {
          Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
          attributes.remove("uri");
          try {
            return Json.asString(attributes);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }));
    PAssert.that(attributesSansUri).containsInAnyOrder(expectedAttributes);

    PCollection<String> uriExceptions = parsed.errors() //
        .apply("ExceptionClass", MapElements.into(TypeDescriptors.strings())
            .via(message -> message.getAttribute("exception_class")));
    PAssert.that(uriExceptions).containsInAnyOrder(expectedExceptions);

    ValueProvider<String> schemas = pipeline.newProvider("schemas.tar.gz");
    ValueProvider<String> aliases = pipeline.newProvider(null);
    PCollection<String> schemaExceptions = output.apply(ParsePayload.of(schemas, aliases)).errors()
        .apply("EncodeJsonErrors", OutputFileFormat.json.encode());
    PAssert.that(schemaExceptions).empty();

    pipeline.run();
  }
}
