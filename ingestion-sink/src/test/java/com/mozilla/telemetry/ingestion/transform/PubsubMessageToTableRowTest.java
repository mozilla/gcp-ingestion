/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.transform;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.io.BigQuery.Write.TableRow;
import com.mozilla.telemetry.ingestion.transform.PubsubMessageToTableRow.TableRowFormat;
import org.junit.Test;

public class PubsubMessageToTableRowTest {

  private static final PubsubMessageToTableRow RAW_TRANSFORM = new PubsubMessageToTableRow(
      "${document_namespace}_raw.${document_type}_v${document_version}", TableRowFormat.raw);

  @Test
  public void canFormatAsRaw() {
    final TableRow actual = RAW_TRANSFORM
        .apply(PubsubMessage.newBuilder().setData(ByteString.copyFrom("test".getBytes()))
            .putAttributes("document_id", "id").putAttributes("document_namespace", "telemetry")
            .putAttributes("document_type", "main").putAttributes("document_version", "4").build());

    assertEquals("telemetry_raw", actual.tableId.getDataset());
    assertEquals("main_v4", actual.tableId.getTable());
    assertEquals(104, actual.byteSize);
    assertEquals(ImmutableMap.of("document_id", "id", "document_namespace", "telemetry",
        "document_type", "main", "document_version", "4", "payload", "dGVzdA=="), actual.content);
  }

  @Test
  public void canFormatAsRawWithEmptyValues() {
    final TableRow actual = RAW_TRANSFORM
        .apply(PubsubMessage.newBuilder().putAttributes("document_namespace", "")
            .putAttributes("document_type", "").putAttributes("document_version", "").build());

    assertEquals("_raw", actual.tableId.getDataset());
    assertEquals("_v", actual.tableId.getTable());
    assertEquals(65, actual.byteSize);
    assertEquals(ImmutableMap.of("document_namespace", "", "document_type", "", "document_version",
        "", "payload", ""), actual.content);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnMissingAttributes() {
    RAW_TRANSFORM.apply(PubsubMessage.newBuilder().build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnInvalidTable() {
    new PubsubMessageToTableRow("", TableRowFormat.raw).apply(PubsubMessage.newBuilder().build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnUnimplementedFormat() {
    new PubsubMessageToTableRow("dataset.table", TableRowFormat.payload)
        .apply(PubsubMessage.newBuilder().build());
  }

  @Test(expected = NullPointerException.class)
  public void failsOnNullMessage() {
    RAW_TRANSFORM.apply(null);
  }

  private static final PubsubMessageToTableRow DECODED_TRANSFORM = new PubsubMessageToTableRow(
      "${document_namespace}_decoded.${document_type}_v${document_version}",
      TableRowFormat.decoded);

  @Test
  public void canFormatTelemetryAsDecoded() {
    final TableRow actual = DECODED_TRANSFORM.apply(PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom("test".getBytes())).putAttributes("geo_city", "geo_city")
        .putAttributes("geo_country", "geo_country")
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

    assertEquals("telemetry_decoded", actual.tableId.getDataset());
    assertEquals("document_type_vdocument_version", actual.tableId.getTable());
    assertEquals(916, actual.byteSize);

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
        actual.content);
  }

  @Test
  public void canFormatNonTelemetryAsDecoded() {
    final TableRow actual = DECODED_TRANSFORM.apply(PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom("test".getBytes())).putAttributes("geo_city", "geo_city")
        .putAttributes("geo_country", "geo_country")
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

    assertEquals("document_namespace_decoded", actual.tableId.getDataset());
    assertEquals("document_type_vdocument_version", actual.tableId.getTable());
    assertEquals(925, actual.byteSize);

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
        actual.content);
  }

  @Test
  public void canFormatAsDecodedWithEmptyValues() {
    final TableRow actual = DECODED_TRANSFORM.apply(PubsubMessage.newBuilder()
        .putAttributes("document_namespace", "").putAttributes("document_type", "")
        .putAttributes("document_version", "").putAttributes("sample_id", "").build());

    assertEquals("_decoded", actual.tableId.getDataset());
    assertEquals("_v", actual.tableId.getTable());
    assertEquals(80, actual.byteSize);
    assertEquals(
        ImmutableMap.of("payload", "", "metadata",
            ImmutableMap.builder().put("document_namespace", "").put("document_type", "")
                .put("document_version", "").put("geo", ImmutableMap.of())
                .put("header", ImmutableMap.of()).put("user_agent", ImmutableMap.of()).build()),
        actual.content);
  }
}
