package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BigQueryDataset;
import com.mozilla.telemetry.ingestion.sink.util.BoundedSink;
import com.mozilla.telemetry.ingestion.sink.util.PubsubTopics;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SinkBigQueryStreamingIntegrationTest {

  // allow inheriting classes to use a different number of topics
  protected int numTopics() {
    return 1;
  }

  @Rule
  public final PubsubTopics pubsub = new PubsubTopics(numTopics());

  @Rule
  public final BigQueryDataset bq = new BigQueryDataset();

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  private static final TimePartitioning TIME_PARTITIONING = TimePartitioning
      .newBuilder(TimePartitioning.Type.DAY).setField("submission_timestamp").build();

  private static final Clustering CLUSTERING = Clustering.newBuilder()
      .setFields(ImmutableList.of("submission_timestamp")).build();

  private static final StandardTableDefinition TABLE_DEF = StandardTableDefinition
      .of(Schema.of(Field.of("document_type", LegacySQLTypeName.STRING),
          Field.of("document_version", LegacySQLTypeName.STRING),
          Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP),
          Field.of("payload", LegacySQLTypeName.BYTES)))
      .toBuilder().setTimePartitioning(TIME_PARTITIONING).setClustering(CLUSTERING).build();

  static final String SUBMISSION_TIMESTAMP = ZonedDateTime.now(ZoneOffset.UTC)
      .format(DateTimeFormatter.ISO_DATE_TIME);

  protected List<PubsubMessage> getInputs() {
    return ImmutableList.of(//
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("data")) //
            .putAttributes("document_type", "test") //
            .putAttributes("document_version", "1") //
            .putAttributes("submission_timestamp", SUBMISSION_TIMESTAMP) //
            .build(),
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("data2")) //
            .putAttributes("document_type", "test") //
            .putAttributes("document_version", "2") //
            .putAttributes("submission_timestamp", SUBMISSION_TIMESTAMP) //
            .build());
  }

  protected List<List<String>> getExpected() {
    return ImmutableList.of(//
        ImmutableList.of("test", "1", SUBMISSION_TIMESTAMP, "data", "test_v1"),
        ImmutableList.of("test", "2", SUBMISSION_TIMESTAMP, "data2", "test_v2"));
  }

  final List<PubsubMessage> inputs = getInputs();
  private final List<List<String>> expected = getExpected();

  void runTest() throws Exception {
    inputs.forEach(message -> {
      String table = "test_v" + message.getAttributesOrThrow("document_version");
      TableId tableId = TableId.of(bq.dataset, table);
      bq.bigquery.create(TableInfo.newBuilder(tableId, TABLE_DEF).build());
      pubsub.publish(0, message);
    });

    BoundedSink.run(inputs.size(), 30);

    final String query = "SELECT * REPLACE (" //
        + "FORMAT_TIMESTAMP('%FT%R:%E*SZ', submission_timestamp) AS submission_timestamp, "
        + "CAST(payload AS STRING) AS payload), " //
        + "_TABLE_SUFFIX AS table " //
        + String.format("FROM `%s.%s.*`", bq.project, bq.dataset) //
        + " ORDER BY document_version";

    // allow retries to handle possible delayed availability of streaming insert
    List<List<String>> actual = null;
    for (int attempt = 0; attempt < 2; attempt++) {
      actual = StreamSupport
          .stream(bq.bigquery.query(QueryJobConfiguration.of(query)).iterateAll().spliterator(),
              false)
          .map(row -> row.stream().map(FieldValue::getStringValue).collect(Collectors.toList()))
          .collect(Collectors.toList());
      if (actual.size() == expected.size()) {
        break;
      }
    }

    assertEquals(expected, actual);
  }

  @Test
  public void canSinkMessages() throws Exception {
    environmentVariables.set("BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(0));
    environmentVariables.set("OUTPUT_TABLE",
        bq.project + "." + bq.dataset + ".${document_type}_v${document_version}");
    runTest();
  }
}
