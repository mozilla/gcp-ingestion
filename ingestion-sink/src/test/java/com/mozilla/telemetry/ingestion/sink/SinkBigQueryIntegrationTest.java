/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.sink;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.TestWithPubsubResources;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.Timeout;

public class SinkBigQueryIntegrationTest extends TestWithPubsubResources {

  private BigQuery bigquery;
  private String project;
  private String dataset;

  /** Find credentials in the environment and create a dataset in BigQuery. */
  @Before
  public void createBigQueryDataset() {
    RemoteBigQueryHelper bqHelper = RemoteBigQueryHelper.create();
    bigquery = bqHelper.getOptions().getService();
    project = bqHelper.getOptions().getProjectId();
    dataset = RemoteBigQueryHelper.generateDatasetName();
    bigquery.create(DatasetInfo.newBuilder(dataset).build());
  }

  /** Remove all resources we created in BigQuery. */
  @After
  public void deleteBigQueryDataset() {
    RemoteBigQueryHelper.forceDelete(bigquery, dataset);
  }

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Rule
  public final Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

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
      TableId tableId = TableId.of(dataset, table);
      bigquery.create(TableInfo.newBuilder(tableId, tableDef).build());
      PubsubMessage.Builder message = PubsubMessage.newBuilder()
          .setData(ByteString.copyFrom("test".getBytes())).putAttributes("document_type", "test")
          .putAttributes("document_version", version)
          .putAttributes("submission_timestamp", submissionTimestamp);
      if (documentIds.containsKey(version)) {
        message.putAttributes("document_id", documentIds.get(version));
      }
      try {
        publisher.publish(message.build()).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    environmentVariables.set("INPUT_SUBSCRIPTION", subscriptionName.toString());
    environmentVariables.set("OUTPUT_TABLE",
        project + "." + dataset + ".${document_type}_v${document_version}");

    CompletableFuture<Void> main = CompletableFuture.runAsync(() -> Sink.main(new String[] {}));

    String query = "SELECT * REPLACE (FORMAT_TIMESTAMP('%FT%R:%E*SZ', submission_timestamp) AS "
        + "submission_timestamp), _TABLE_SUFFIX AS table FROM "
        + String.format("`%s.%s.*`", project, dataset);
    AtomicReference<List<List<String>>> actual = new AtomicReference<>();
    do {
      actual.set(new LinkedList<>());
      bigquery.query(QueryJobConfiguration.of(query)).iterateAll()
          .forEach(row -> actual.get().add(row.stream().map(fv -> {
            try {
              return fv.getStringValue();
            } catch (NullPointerException e) {
              return "";
            }
          }).collect(Collectors.toList())));
    } while (actual.get().size() < versions.size());
    assertEquals(versions.size(), actual.get().size());

    main.cancel(true);
    try {
      main.join();
    } catch (CancellationException ignore) {
      // ignore
    }

    List<List<String>> expected = ImmutableList.of(
        ImmutableList.of(documentIds.get("1"), "test", "1", submissionTimestamp, "test_v1"),
        ImmutableList.of("", "test", "2", submissionTimestamp, "test_v2"));
    actual.get().sort(Comparator.comparing(row -> row.get(4)));
    assertEquals(expected, actual.get());
  }
}
