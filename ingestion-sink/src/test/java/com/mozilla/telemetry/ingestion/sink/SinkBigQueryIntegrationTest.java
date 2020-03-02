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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BigQueryDataset;
import com.mozilla.telemetry.ingestion.sink.util.BoundedSink;
import com.mozilla.telemetry.ingestion.sink.util.GcsBucket;
import com.mozilla.telemetry.ingestion.sink.util.PubsubTopics;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SinkBigQueryIntegrationTest {

  @Rule
  public final GcsBucket gcs = new GcsBucket();

  @Rule
  public final PubsubTopics pubsub = new PubsubTopics(2);

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

  private void createTablesAndPublishInputs(Iterable<PubsubMessage> inputs) {
    inputs.forEach(message -> {
      String table = message.getAttributesOrThrow("document_type") + "_v"
          + message.getAttributesOrThrow("document_version");
      TableId tableId = TableId.of(bq.dataset, table);
      bq.bigquery.create(TableInfo.newBuilder(tableId, TABLE_DEF).build());
      pubsub.publish(0, message);
    });
  }

  private static final Comparator<List<String>> ROW_COMPARATOR = Comparator
      .comparing(row -> row.get(row.size() - 1));

  private void checkResult(Iterable<List<String>> expected) throws Exception {
    final List<List<String>> sorted = ImmutableList.sortedCopyOf(ROW_COMPARATOR, expected);

    final String query = "SELECT * REPLACE (" //
        + "FORMAT_TIMESTAMP('%FT%R:%E*SZ', submission_timestamp) AS submission_timestamp, "
        + "CAST(payload AS STRING) AS payload), " //
        + "_TABLE_SUFFIX AS table " //
        + String.format("FROM `%s.%s.*`", bq.project, bq.dataset) //
        + " ORDER BY table";

    // allow retries to handle possible delayed availability of streaming insert
    List<List<String>> actual = null;
    for (int attempt = 0; attempt < 2; attempt++) {
      actual = StreamSupport
          .stream(bq.bigquery.query(QueryJobConfiguration.of(query)).iterateAll().spliterator(),
              false)
          .map(row -> row.stream().map(FieldValue::getStringValue).collect(Collectors.toList()))
          .collect(Collectors.toList());
      if (actual.size() == sorted.size()) {
        break;
      }
    }

    assertEquals(sorted, actual);
  }

  private static final String SUBMISSION_TIMESTAMP = ZonedDateTime.now(ZoneOffset.UTC)
      .format(DateTimeFormatter.ISO_DATE_TIME);

  private static final PubsubMessage SIMPLE_MESSAGE = PubsubMessage.newBuilder() //
      .setData(ByteString.copyFromUtf8("x")) //
      .putAttributes("document_type", "foo") //
      .putAttributes("document_version", "1") //
      .putAttributes("submission_timestamp", SUBMISSION_TIMESTAMP) //
      .build();
  private static final List<String> SIMPLE_MESSAGE_ROW = ImmutableList.of("foo", "1",
      SUBMISSION_TIMESTAMP, "x", "foo_v1");

  @Test
  public void canStream() throws Exception {
    createTablesAndPublishInputs(ImmutableList.of(SIMPLE_MESSAGE));

    environmentVariables.set("BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(0));
    environmentVariables.set("OUTPUT_TABLE",
        bq.project + "." + bq.dataset + ".${document_type}_v${document_version}");
    BoundedSink.run(1, 30);
    checkResult(ImmutableList.of(SIMPLE_MESSAGE_ROW));
  }

  @Test
  public void canFileLoad() throws Exception {
    createTablesAndPublishInputs(ImmutableList.of(SIMPLE_MESSAGE));

    Map<String, String> sinkEnv = ImmutableMap.<String, String>builder()
        .put("BATCH_MAX_DELAY", "0.001s") //
        .put("BIG_QUERY_OUTPUT_MODE", "file_loads") //
        .put("INPUT_SUBSCRIPTION", pubsub.getSubscription(0)) //
        .put("OUTPUT_BUCKET", gcs.bucket) //
        .put("OUTPUT_TABLE",
            bq.project + "." + bq.dataset + ".${document_type}_v${document_version}") //
        .put("OUTPUT_TOPIC", pubsub.getTopic(1)) //
        .build();
    sinkEnv.forEach(environmentVariables::set);
    BoundedSink.run(1, 30);
    checkResult(ImmutableList.of());

    sinkEnv.keySet().forEach(environmentVariables::clear);
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(1));
    environmentVariables.set("LOAD_MAX_DELAY", "0.001s");
    // unregister stackdriver stats exporter so BoundedSink can register another one
    StackdriverStatsExporter.unregister();
    BoundedSink.run(1, 30);
    checkResult(ImmutableList.of(SIMPLE_MESSAGE_ROW));
  }

  private static final String OVERSIZE_ROW_DATA = Strings.repeat("x", 2 * 1024 * 1024);
  private static final String OVERSIZE_REQUEST_DATA = Strings.repeat("x", 8 * 1024 * 1024);

  @Test
  public void canFallBackToFileLoad() throws Exception {
    final List<PubsubMessage> input = ImmutableList.of(
        SIMPLE_MESSAGE.toBuilder().setData(ByteString.copyFromUtf8(OVERSIZE_ROW_DATA))
            .putAttributes("document_type", "bar").build(),
        SIMPLE_MESSAGE.toBuilder().setData(ByteString.copyFromUtf8(OVERSIZE_REQUEST_DATA)).build());
    createTablesAndPublishInputs(input);

    environmentVariables.set("BATCH_MAX_DELAY", "0.001s");
    environmentVariables.set("BIG_QUERY_OUTPUT_MODE", "mixed");
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(0));
    environmentVariables.set("LOAD_MAX_DELAY", "0.001s");
    environmentVariables.set("OUTPUT_BUCKET", gcs.bucket);
    environmentVariables.set("OUTPUT_TABLE",
        bq.project + "." + bq.dataset + ".${document_type}_v${document_version}");
    environmentVariables.set("STREAMING_BATCH_MAX_DELAY", "0.001s");
    BoundedSink.run(input.size(), 30);
    checkResult(ImmutableList.of(
        ImmutableList.of("bar", "1", SUBMISSION_TIMESTAMP, OVERSIZE_ROW_DATA, "bar_v1"),
        ImmutableList.of("foo", "1", SUBMISSION_TIMESTAMP, OVERSIZE_REQUEST_DATA, "foo_v1")));
  }

  @Test
  public void canStreamSpecificDoctypes() throws Exception {
    final List<PubsubMessage> streamingInput = ImmutableList.of(
        SIMPLE_MESSAGE.toBuilder().putAttributes("document_namespace", "namespace_0").build(),
        SIMPLE_MESSAGE.toBuilder().putAttributes("document_namespace", "namespace_0")
            .putAttributes("document_type", "bar").build(),
        SIMPLE_MESSAGE.toBuilder().putAttributes("document_namespace", "namespace_1")
            .putAttributes("document_version", "2").build());
    final List<List<String>> streamingExpected = ImmutableList.of(
        ImmutableList.of("foo", "1", SUBMISSION_TIMESTAMP, "x", "foo_v1"),
        ImmutableList.of("bar", "1", SUBMISSION_TIMESTAMP, "x", "bar_v1"),
        ImmutableList.of("foo", "2", SUBMISSION_TIMESTAMP, "x", "foo_v2"));
    createTablesAndPublishInputs(streamingInput);

    final List<PubsubMessage> nonStreamingInput = ImmutableList.of(
        SIMPLE_MESSAGE.toBuilder().putAttributes("document_namespace", "namespace_1")
            .putAttributes("document_type", "bar").putAttributes("document_version", "2").build(),
        SIMPLE_MESSAGE.toBuilder().putAttributes("document_namespace", "namespace_2")
            .putAttributes("document_version", "3").build(),
        SIMPLE_MESSAGE.toBuilder().putAttributes("document_namespace", "namespace_2")
            .putAttributes("document_type", "bar").putAttributes("document_version", "3").build());
    final List<List<String>> nonStreamingExpected = ImmutableList.of(
        ImmutableList.of("bar", "2", SUBMISSION_TIMESTAMP, "x", "bar_v2"),
        ImmutableList.of("foo", "3", SUBMISSION_TIMESTAMP, "x", "foo_v3"),
        ImmutableList.of("bar", "3", SUBMISSION_TIMESTAMP, "x", "bar_v3"));
    createTablesAndPublishInputs(nonStreamingInput);

    Map<String, String> sinkEnv = ImmutableMap.<String, String>builder()
        .put("BATCH_MAX_DELAY", "0.001s") //
        .put("BIG_QUERY_OUTPUT_MODE", "mixed") //
        .put("INPUT_SUBSCRIPTION", pubsub.getSubscription(0)) //
        .put("OUTPUT_BUCKET", gcs.bucket) //
        .put("OUTPUT_TABLE",
            bq.project + "." + bq.dataset + ".${document_type}_v${document_version}") //
        .put("OUTPUT_TOPIC", pubsub.getTopic(1)) //
        .put("STREAMING_BATCH_MAX_DELAY", "0.001s") //
        .put("STREAMING_DOCTYPES", "namespace-0/.*|namespace-1/foo") //
        .build();
    sinkEnv.forEach(environmentVariables::set);
    BoundedSink.run(streamingInput.size() + nonStreamingInput.size(), 30);
    checkResult(streamingExpected);

    sinkEnv.keySet().forEach(environmentVariables::clear);
    environmentVariables.set("INPUT_SUBSCRIPTION", pubsub.getSubscription(1));
    environmentVariables.set("LOAD_MAX_DELAY", "0.001s");
    // unregister stackdriver stats exporter so BoundedSink can register another one
    StackdriverStatsExporter.unregister();
    BoundedSink.run(nonStreamingInput.size(), 30);
    checkResult(Iterables.concat(streamingExpected, nonStreamingExpected));
  }
}
