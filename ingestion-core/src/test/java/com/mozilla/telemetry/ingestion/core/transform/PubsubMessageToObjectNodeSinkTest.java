package com.mozilla.telemetry.ingestion.core.transform;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.FieldName;
import com.mozilla.telemetry.ingestion.core.schema.TestConstant;
import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import com.mozilla.telemetry.ingestion.core.util.Json;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public class PubsubMessageToObjectNodeSinkTest {

  private static final PubsubMessageToObjectNode RAW_TRANSFORM = PubsubMessageToObjectNode.Raw.of();
  private static final PubsubMessageToObjectNode DECODED_TRANSFORM = PubsubMessageToObjectNode //
      .Decoded.of();
  private static final Map<String, String> EMPTY_ATTRIBUTES = ImmutableMap.of();
  private static final byte[] EMPTY_DATA = new byte[] {};

  @Test
  public void canFormatAsRaw() {
    final ObjectNode actual = RAW_TRANSFORM.apply(ImmutableMap.of("document_id", "id",
        "document_namespace", "telemetry", "document_type", "main", "document_version", "4"),
        "test".getBytes(StandardCharsets.UTF_8));
    assertEquals(ImmutableMap.of("document_id", "id", "document_namespace", "telemetry",
        "document_type", "main", "document_version", "4", "payload", "dGVzdA=="),
        Json.asMap(actual));
  }

  @Test
  public void canFormatAsRawWithEmptyValues() {
    assertEquals(ImmutableMap.of(), Json.asMap(RAW_TRANSFORM.apply(EMPTY_ATTRIBUTES, EMPTY_DATA)));
  }

  @Test(expected = ClassCastException.class)
  public void failsFormatRawNullMessage() {
    RAW_TRANSFORM.apply(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void failsFormatDecodedNullMessage() {
    DECODED_TRANSFORM.apply(null, null);
  }

  @Test
  public void canFormatTelemetryAsDecoded() {
    final ObjectNode actual = DECODED_TRANSFORM.apply(ImmutableMap.<String, String>builder()
        .put("geo_city", "geo_city").put("geo_country", "geo_country")
        .put("geo_subdivision1", "geo_subdivision1").put("geo_subdivision2", "geo_subdivision2")
        .put("isp_db_version", "isp_db_version").put("isp_name", "isp_name")
        .put("isp_organization", "isp_organization").put("user_agent_browser", "user_agent_browser")
        .put("user_agent_os", "user_agent_os").put("user_agent_version", "user_agent_version")
        .put("date", "date").put("dnt", "dnt").put("x_debug_id", "x_debug_id")
        .put("x_pingsender_version", "x_pingsender_version").put("uri", "uri")
        .put("app_build_id", "app_build_id").put("app_name", "app_name")
        .put("app_update_channel", "app_update_channel").put("app_version", "app_version")
        .put("document_namespace", "telemetry").put("document_type", "document_type")
        .put("document_version", "document_version")
        .put("submission_timestamp", "submission_timestamp").put("document_id", "document_id")
        .put("normalized_app_name", "normalized_app_name")
        .put("normalized_channel", "normalized_channel")
        .put("normalized_country_code", "normalized_country_code")
        .put("normalized_os", "normalized_os").put("normalized_os_version", "normalized_os_version")
        .put("sample_id", "42").put("client_id", "client_id").build(),
        "test".getBytes(StandardCharsets.UTF_8));

    assertEquals(
        ImmutableMap.builder()
            .put("metadata", ImmutableMap.builder()
                .put("geo",
                    ImmutableMap.builder().put("country", "geo_country").put("city", "geo_city")
                        .put("subdivision2", "geo_subdivision2")
                        .put("subdivision1", "geo_subdivision1").build())
                .put("isp",
                    ImmutableMap.builder().put("db_version", "isp_db_version")
                        .put("name", "isp_name").put("organization", "isp_organization").build())
                .put("user_agent",
                    ImmutableMap.builder().put("browser", "user_agent_browser")
                        .put("os", "user_agent_os").put("version", "user_agent_version").build())
                .put("header",
                    ImmutableMap.builder().put("date", "date").put("dnt", "dnt")
                        .put("x_debug_id", "x_debug_id")
                        .put("x_pingsender_version", "x_pingsender_version").build())
                .put("uri",
                    ImmutableMap.builder().put("uri", "uri").put("app_build_id", "app_build_id")
                        .put("app_name", "app_name").put("app_update_channel", "app_update_channel")
                        .put("app_version", "app_version").build())
                .put("document_namespace", "telemetry").put("document_type", "document_type")
                .put("document_version", "document_version").build())
            .put("submission_timestamp", "submission_timestamp").put("document_id", "document_id")
            .put("normalized_app_name", "normalized_app_name")
            .put("normalized_channel", "normalized_channel")
            .put("normalized_country_code", "normalized_country_code")
            .put("normalized_os", "normalized_os")
            .put("normalized_os_version", "normalized_os_version").put("sample_id", 42)
            .put("payload", "dGVzdA==").put("client_id", "client_id").build(),
        Json.asMap(actual));
  }

  @Test
  public void canFormatNonTelemetryAsDecoded() {
    final ObjectNode actual = DECODED_TRANSFORM.apply(ImmutableMap.<String, String>builder()
        .put("geo_city", "geo_city").put("geo_country", "geo_country")
        .put("geo_subdivision1", "geo_subdivision1").put("geo_subdivision2", "geo_subdivision2")
        .put("isp_db_version", "isp_db_version").put("isp_name", "isp_name")
        .put("isp_organization", "isp_organization").put("user_agent_browser", "user_agent_browser")
        .put("user_agent_os", "user_agent_os").put("user_agent_version", "user_agent_version")
        .put("date", "date").put("dnt", "dnt").put("x_debug_id", "x_debug_id")
        .put("x_pingsender_version", "x_pingsender_version").put("uri", "uri")
        .put("app_build_id", "app_build_id").put("app_name", "app_name")
        .put("app_update_channel", "app_update_channel").put("app_version", "app_version")
        .put("document_namespace", "document_namespace").put("document_type", "document_type")
        .put("document_version", "document_version")
        .put("submission_timestamp", "submission_timestamp").put("document_id", "document_id")
        .put("normalized_app_name", "normalized_app_name")
        .put("normalized_channel", "normalized_channel")
        .put("normalized_country_code", "normalized_country_code")
        .put("normalized_os", "normalized_os").put("normalized_os_version", "normalized_os_version")
        .put("sample_id", "42").put("client_id", "client_id").build(),
        "test".getBytes(StandardCharsets.UTF_8));

    assertEquals(
        ImmutableMap.builder()
            .put("metadata", ImmutableMap.builder()
                .put("geo",
                    ImmutableMap.builder().put("country", "geo_country").put("city", "geo_city")
                        .put("subdivision2", "geo_subdivision2")
                        .put("subdivision1", "geo_subdivision1").build())
                .put("isp",
                    ImmutableMap.builder().put("db_version", "isp_db_version")
                        .put("name", "isp_name").put("organization", "isp_organization").build())
                .put("user_agent",
                    ImmutableMap.builder().put("browser", "user_agent_browser")
                        .put("os", "user_agent_os").put("version", "user_agent_version").build())
                .put("header",
                    ImmutableMap.builder().put("date", "date").put("dnt", "dnt")
                        .put("x_debug_id", "x_debug_id")
                        .put("x_pingsender_version", "x_pingsender_version").build())
                .put("document_namespace", "document_namespace")
                .put("document_type", "document_type").put("document_version", "document_version")
                .build())
            .put("submission_timestamp", "submission_timestamp").put("document_id", "document_id")
            .put("normalized_app_name", "normalized_app_name")
            .put("normalized_channel", "normalized_channel")
            .put("normalized_country_code", "normalized_country_code")
            .put("normalized_os", "normalized_os")
            .put("normalized_os_version", "normalized_os_version").put("sample_id", 42)
            .put("payload", "dGVzdA==").put("client_id", "client_id").build(),
        Json.asMap(actual));
  }

  @Test
  public void canFormatTelemetryAsDecodedWithEmptyValues() {
    final ObjectNode actual = DECODED_TRANSFORM
        .apply(ImmutableMap.of("document_namespace", "telemetry"), EMPTY_DATA);

    assertEquals(ImmutableMap.builder().put("metadata",
        ImmutableMap.builder().put("document_namespace", "telemetry").put("geo", ImmutableMap.of())
            .put("isp", ImmutableMap.of()).put("header", ImmutableMap.of())
            .put("user_agent", ImmutableMap.of()).put("uri", ImmutableMap.of()).build())
        .build(), Json.asMap(actual));
  }

  @Test
  public void canFormatNonTelemetryAsDecodedWithEmptyValues() {
    assertEquals(ImmutableMap.builder()
        .put("metadata",
            ImmutableMap.builder().put("geo", ImmutableMap.of()).put("isp", ImmutableMap.of())
                .put("header", ImmutableMap.of()).put("user_agent", ImmutableMap.of()).build())
        .build(), Json.asMap(DECODED_TRANSFORM.apply(EMPTY_ATTRIBUTES, EMPTY_DATA)));
  }

  private static TypeReference<Map<String, String>> stringMap = //
      new TypeReference<Map<String, String>>() {
      };

  private static Stream<Map.Entry<Map<String, String>, byte[]>> asPubsubMessage(ObjectNode node)
      throws IOException {
    return ImmutableMap.of(Json.convertValue(node.get("attributeMap"), stringMap),
        Json.asBytes(node.get(FieldName.PAYLOAD))).entrySet().stream();
  }

  @Test
  public void canFormatAsPayload() throws IOException {
    PubsubMessageToObjectNode transform = PubsubMessageToObjectNode //
        .Payload.of(ImmutableList.of("namespace_0/foo"), TestConstant.SCHEMAS_LOCATION,
            FileInputStream::new);

    final List<Map.Entry<Map<String, String>, byte[]>> input;
    final String inputPath = Resources.getResource("testdata/payload-format-input.ndjson")
        .getPath();
    try (Stream<String> stream = Files.lines(Paths.get(inputPath))) {
      input = stream.map(IOFunction.unchecked(Json::readObjectNode))
          .flatMap(IOFunction.unchecked(PubsubMessageToObjectNodeSinkTest::asPubsubMessage))
          .collect(Collectors.toList());
    }

    final List<Map<String, Object>> expected;
    final String expectedPath = Resources.getResource("testdata/payload-format-expected.ndjson")
        .getPath();
    try (Stream<String> stream = Files.lines(Paths.get(expectedPath))) {
      expected = stream.map(IOFunction.unchecked(Json::readObjectNode)).map(node -> {
        // convert additional_properties to a json string if present
        if (node.hasNonNull(FieldName.ADDITIONAL_PROPERTIES)) {
          node.put(FieldName.ADDITIONAL_PROPERTIES,
              Json.asString(node.get(FieldName.ADDITIONAL_PROPERTIES)));
        }
        return node;
      }).map(IOFunction.unchecked(Json::asMap)).collect(Collectors.toList());
    }

    assertEquals(expected.size(), input.size());

    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i),
          Json.asMap(transform.apply(input.get(i).getKey(), input.get(i).getValue())));
    }
  }
}
