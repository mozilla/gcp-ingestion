package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BigQueryDataset;
import com.mozilla.telemetry.ingestion.sink.util.BoundedSink;
import com.mozilla.telemetry.ingestion.sink.util.SinglePubsubTopic;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SinkBigQueryIntegrationTest {

  @Rule
  public final SinglePubsubTopic pubsub = new SinglePubsubTopic();

  @Rule
  public final BigQueryDataset bq = new BigQueryDataset();

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  private static final TimePartitioning TIME_PARTITIONING = TimePartitioning
      .newBuilder(TimePartitioning.Type.DAY).setField("submission_timestamp").build();

  private static final Clustering CLUSTERING = Clustering.newBuilder()
      .setFields(ImmutableList.of("submission_timestamp")).build();

  @Test
  public void canSinkRawMessages() throws Exception {
    StandardTableDefinition tableDef = StandardTableDefinition
        .of(Schema.of(Field.of("document_id", LegacySQLTypeName.STRING),
            Field.of("document_type", LegacySQLTypeName.STRING),
            Field.of("document_version", LegacySQLTypeName.STRING),
            Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP)))
        .toBuilder().setTimePartitioning(TIME_PARTITIONING).setClustering(CLUSTERING).build();

    String submissionTimestamp = ZonedDateTime.now(ZoneOffset.UTC)
        .format(DateTimeFormatter.ISO_DATE_TIME);
    Map<String, String> documentIds = ImmutableMap.of("1", UUID.randomUUID().toString());
    List<String> versions = ImmutableList.of("1", "2");
    versions.forEach(version -> {
      String table = "test_v" + version;
      TableId tableId = TableId.of(bq.dataset, table);
      bq.bigquery.create(TableInfo.newBuilder(tableId, tableDef).build());
      PubsubMessage.Builder message = PubsubMessage.newBuilder()
          .setData(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
          .putAttributes("document_type", "test").putAttributes("document_version", version)
          .putAttributes("submission_timestamp", submissionTimestamp);
      if (documentIds.containsKey(version)) {
        message.putAttributes("document_id", documentIds.get(version));
      }
      pubsub.publish(message.build());
    });

    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription());
    environmentVariables.set("OUTPUT_TABLE",
        bq.project + "." + bq.dataset + ".${document_type}_v${document_version}");

    BoundedSink.run(versions.size(), 30);

    String query = "SELECT * REPLACE (FORMAT_TIMESTAMP('%FT%R:%E*SZ', submission_timestamp) AS "
        + "submission_timestamp), _TABLE_SUFFIX AS table FROM "
        + String.format("`%s.%s.*`", bq.project, bq.dataset);
    List<List<String>> actual = new LinkedList<>();
    bq.bigquery.query(QueryJobConfiguration.of(query)).iterateAll()
        .forEach(row -> actual.add(row.stream().map(fv -> {
          try {
            return fv.getStringValue();
          } catch (NullPointerException e) {
            return "";
          }
        }).collect(Collectors.toList())));

    List<List<String>> expected = ImmutableList.of(
        ImmutableList.of(documentIds.get("1"), "test", "1", submissionTimestamp, "test_v1"),
        ImmutableList.of("", "test", "2", submissionTimestamp, "test_v2"));
    actual.sort(Comparator.comparing(row -> row.get(4)));
    assertEquals(expected, actual);
  }
}
