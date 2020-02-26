package com.mozilla.telemetry.ingestion.sink.transform;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
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

public class PubsubMessageToObjectNodeTest {

  private static final PubsubMessageToObjectNode RAW_TRANSFORM = PubsubMessageToObjectNode.Raw.of();
  private static final PubsubMessageToObjectNode DECODED_TRANSFORM = PubsubMessageToObjectNode //
      .Decoded.of();
  private static final PubsubMessage EMPTY_MESSAGE = PubsubMessage.newBuilder().build();

  @Test
  public void canFormatAsRaw() {
    final ObjectNode actual = RAW_TRANSFORM.apply(PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
        .putAttributes("document_id", "id").putAttributes("document_namespace", "telemetry")
        .putAttributes("document_type", "main").putAttributes("document_version", "4").build());
    assertEquals(ImmutableMap.of("document_id", "id", "document_namespace", "telemetry",
        "document_type", "main", "document_version", "4", "payload", "dGVzdA=="),
        Json.asMap(actual));
  }

  @Test
  public void canFormatAsRawWithEmptyValues() {
    assertEquals(ImmutableMap.of(), Json.asMap(RAW_TRANSFORM.apply(EMPTY_MESSAGE)));
  }

  @Test(expected = NullPointerException.class)
  public void failsFormatRawNullMessage() {
    RAW_TRANSFORM.apply(null);
  }

  @Test(expected = NullPointerException.class)
  public void failsFormatDecodedNullMessage() {
    DECODED_TRANSFORM.apply(null);
  }

  @Test
  public void canFormatTelemetryAsDecoded() {
    final ObjectNode actual = DECODED_TRANSFORM.apply(PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
        .putAttributes("geo_city", "geo_city").putAttributes("geo_country", "geo_country")
        .putAttributes("geo_subdivision1", "geo_subdivision1")
        .putAttributes("geo_subdivision2", "geo_subdivision2")
        .putAttributes("user_agent_browser", "user_agent_browser")
        .putAttributes("user_agent_os", "user_agent_os")
        .putAttributes("user_agent_version", "user_agent_version").putAttributes("date", "date")
        .putAttributes("dnt", "dnt").putAttributes("x_debug_id", "x_debug_id")
        .putAttributes("x_pingsender_version", "x_pingsender_version").putAttributes("uri", "uri")
        .putAttributes("app_build_id", "app_build_id").putAttributes("app_name", "app_name")
        .putAttributes("app_update_channel", "app_update_channel")
        .putAttributes("app_version", "app_version")
        .putAttributes("document_namespace", "telemetry")
        .putAttributes("document_type", "document_type")
        .putAttributes("document_version", "document_version")
        .putAttributes("submission_timestamp", "submission_timestamp")
        .putAttributes("document_id", "document_id")
        .putAttributes("normalized_app_name", "normalized_app_name")
        .putAttributes("normalized_channel", "normalized_channel")
        .putAttributes("normalized_country_code", "normalized_country_code")
        .putAttributes("normalized_os", "normalized_os")
        .putAttributes("normalized_os_version", "normalized_os_version")
        .putAttributes("sample_id", "42").putAttributes("client_id", "client_id").build());

    assertEquals(
        ImmutableMap.builder()
            .put("metadata", ImmutableMap.builder()
                .put("geo",
                    ImmutableMap.builder().put("country", "geo_country").put("city", "geo_city")
                        .put("subdivision2", "geo_subdivision2")
                        .put("subdivision1", "geo_subdivision1").build())
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
    final ObjectNode actual = DECODED_TRANSFORM.apply(PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
        .putAttributes("geo_city", "geo_city").putAttributes("geo_country", "geo_country")
        .putAttributes("geo_subdivision1", "geo_subdivision1")
        .putAttributes("geo_subdivision2", "geo_subdivision2")
        .putAttributes("user_agent_browser", "user_agent_browser")
        .putAttributes("user_agent_os", "user_agent_os")
        .putAttributes("user_agent_version", "user_agent_version").putAttributes("date", "date")
        .putAttributes("dnt", "dnt").putAttributes("x_debug_id", "x_debug_id")
        .putAttributes("x_pingsender_version", "x_pingsender_version").putAttributes("uri", "uri")
        .putAttributes("app_build_id", "app_build_id").putAttributes("app_name", "app_name")
        .putAttributes("app_update_channel", "app_update_channel")
        .putAttributes("app_version", "app_version")
        .putAttributes("document_namespace", "document_namespace")
        .putAttributes("document_type", "document_type")
        .putAttributes("document_version", "document_version")
        .putAttributes("submission_timestamp", "submission_timestamp")
        .putAttributes("document_id", "document_id")
        .putAttributes("normalized_app_name", "normalized_app_name")
        .putAttributes("normalized_channel", "normalized_channel")
        .putAttributes("normalized_country_code", "normalized_country_code")
        .putAttributes("normalized_os", "normalized_os")
        .putAttributes("normalized_os_version", "normalized_os_version")
        .putAttributes("sample_id", "42").putAttributes("client_id", "client_id").build());

    assertEquals(
        ImmutableMap.builder()
            .put("metadata", ImmutableMap.builder()
                .put("geo",
                    ImmutableMap.builder().put("country", "geo_country").put("city", "geo_city")
                        .put("subdivision2", "geo_subdivision2")
                        .put("subdivision1", "geo_subdivision1").build())
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
        .apply(PubsubMessage.newBuilder().putAttributes("document_namespace", "telemetry").build());

    assertEquals(ImmutableMap.builder()
        .put("metadata",
            ImmutableMap.builder().put("document_namespace", "telemetry")
                .put("geo", ImmutableMap.of()).put("header", ImmutableMap.of())
                .put("user_agent", ImmutableMap.of()).put("uri", ImmutableMap.of()).build())
        .build(), Json.asMap(actual));
  }

  @Test
  public void canFormatNonTelemetryAsDecodedWithEmptyValues() {
    assertEquals(ImmutableMap.builder()
        .put("metadata",
            ImmutableMap.builder().put("geo", ImmutableMap.of()).put("header", ImmutableMap.of())
                .put("user_agent", ImmutableMap.of()).build())
        .build(), Json.asMap(DECODED_TRANSFORM.apply(EMPTY_MESSAGE)));
  }

  private static TypeReference<Map<String, String>> stringMap = //
      new TypeReference<Map<String, String>>() {
      };

  private static PubsubMessage asPubsubMessage(ObjectNode node) throws IOException {
    return PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom(Json.asBytes(node.get(FieldName.PAYLOAD))))
        .putAllAttributes(Json.convertValue((ObjectNode) node.get("attributeMap"), stringMap))
        .build();
  }

  @Test
  public void canFormatAsPayload() throws IOException {
    PubsubMessageToObjectNode transform = PubsubMessageToObjectNode //
        .Payload.of(ImmutableList.of("namespace_0/foo"), TestConstant.SCHEMAS_LOCATION,
            FileInputStream::new);

    final List<PubsubMessage> input;
    final String inputPath = Resources.getResource("testdata/payload-format-input.ndjson")
        .getPath();
    try (Stream<String> stream = Files.lines(Paths.get(inputPath))) {
      input = stream.map(IOFunction.unchecked(Json::readObjectNode))
          .map(IOFunction.unchecked(PubsubMessageToObjectNodeTest::asPubsubMessage))
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
      assertEquals(expected.get(i), Json.asMap(transform.apply(input.get(i))));
    }
  }
}
