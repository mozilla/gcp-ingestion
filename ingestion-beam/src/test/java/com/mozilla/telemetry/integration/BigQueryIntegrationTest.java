/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.integration;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.mozilla.telemetry.Sink;
import com.mozilla.telemetry.matchers.Lines;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test suite that accesses BigQuery in GCP.
 *
 * <p>Because this requires credentials, this suite is excluded by default in the surefire
 * configuration, but can be enabled by passing command-line option
 * {@code -Dtest=BigQueryIntegrationTest}. Credentials can be provided by initializing a
 * configuration in the gcloud command-line tool or by providing a path to service account
 * credentials in environment variable {@code GOOGLE_APPLICATION_CREDENTIALS}.
 *
 * <p>The provided credentials are assumed to have "BigQuery Admin" privileges.
 */
public class BigQueryIntegrationTest {

  private BigQuery bigquery;
  private String projectId;
  private String dataset;

  /** Find credentials in the environment and create a dataset in BigQuery. */
  @Before
  public void createBigQueryDataset() {
    RemoteBigQueryHelper bqHelper = RemoteBigQueryHelper.create();
    bigquery = bqHelper.getOptions().getService();
    projectId = bqHelper.getOptions().getProjectId();
    dataset = RemoteBigQueryHelper.generateDatasetName();
  }

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();
  private String outputPath;

  @Before
  public void initializeOutputPath() {
    outputPath = outputFolder.getRoot().getAbsolutePath();
  }

  /** Remove all resources we created in BigQuery. */
  @After
  public void deleteBigQueryDataset() {
    RemoteBigQueryHelper.forceDelete(bigquery, dataset);
  }

  private static TimePartitioning submissionTimestampPartitioning = TimePartitioning
      .newBuilder(Type.DAY).setField("submission_timestamp").build();

  @Test
  public void canWriteToBigQuery() throws Exception {
    String table = "mytable";
    String tableSpec = String.format("%s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery
        .create(
            TableInfo
                .newBuilder(tableId,
                    StandardTableDefinition
                        .of(Schema.of(Field.of("clientId", LegacySQLTypeName.STRING),
                            Field.of("type", LegacySQLTypeName.STRING),
                            Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP)))
                        .toBuilder().setTimePartitioning(submissionTimestampPartitioning).build())
                .build());

    String input = Resources.getResource("testdata/bigquery-integration/input.ndjson").getPath();
    String output = String.format("%s:%s", projectId, tableSpec);

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=text", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--bqWriteMethod=streaming",
        "--output=" + output, "--errorOutputType=stderr" });

    result.waitUntilFinish();

    assertThat(stringValuesQuery("SELECT submission_timestamp FROM " + tableSpec),
        matchesInAnyOrder(Lists.newArrayList(null, null, "1561983194.123456")));
    assertThat(stringValuesQuery("SELECT clientId FROM " + tableSpec),
        matchesInAnyOrder(ImmutableList.of("abc123", "abc123", "def456")));
  }

  @Test
  public void canWriteToDynamicTables() throws Exception {
    String table = "my_test_table";
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery.create(TableInfo.newBuilder(tableId,
        StandardTableDefinition.of(Schema.of(Field.of("clientId", LegacySQLTypeName.STRING),
            Field.of("type", LegacySQLTypeName.STRING))))
        .build());

    String input = Resources
        .getResource("testdata/bigquery-integration/input-with-attributes.ndjson").getPath();
    String output = String.format("%s:%s.%s", projectId, dataset, "${document_type}_table");
    String errorOutput = outputPath + "/error/out";

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--output=" + output,
        "--bqWriteMethod=streaming", "--errorOutputType=file",
        "--errorOutputFileCompression=UNCOMPRESSED", "--errorOutput=" + errorOutput });

    result.waitUntilFinish();

    String tableSpec = String.format("%s.%s", dataset, table);
    assertThat(stringValuesQuery("SELECT clientId FROM " + tableSpec),
        matchesInAnyOrder(ImmutableList.of("abc123")));

    List<String> errorOutputLines = Lines.files(outputPath + "/error/out*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(2));
  }

  @Test
  public void canWriteViaFileLoads() throws Exception {
    String table = "my_test_table";
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery
        .create(
            TableInfo
                .newBuilder(tableId,
                    StandardTableDefinition
                        .of(Schema.of(Field.of("clientId", LegacySQLTypeName.STRING),
                            Field.of("type", LegacySQLTypeName.STRING),
                            Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP)))
                        .toBuilder().setTimePartitioning(submissionTimestampPartitioning).build())
                .build());

    String input = Resources
        .getResource("testdata/bigquery-integration/input-with-attributes.ndjson").getPath();
    String output = String.format("%s:%s.%s", projectId, dataset, "${document_type}_table");
    String errorOutput = outputPath + "/error/out";

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--output=" + output,
        "--bqWriteMethod=file_loads", "--errorOutputType=file",
        "--tempLocation=gs://gcp-ingestion-static-test-bucket/temp/bq-loads",
        "--errorOutputFileCompression=UNCOMPRESSED", "--errorOutput=" + errorOutput });

    result.waitUntilFinish();

    String tableSpec = String.format("%s.%s", dataset, table);
    assertThat(stringValuesQuery("SELECT clientId FROM " + tableSpec),
        matchesInAnyOrder(ImmutableList.of("abc123")));

    List<String> errorOutputLines = Lines.files(outputPath + "/error/out*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(2));
  }

  @Test
  public void canRecoverFailedInsertsInStreamingMode() throws Exception {
    String table = "table_with_required_col";
    String tableSpec = String.format("%s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());

    bigquery.create(TableInfo.newBuilder(tableId,
        StandardTableDefinition.of(Schema.of(Field.of("clientId", LegacySQLTypeName.STRING),
            Field.newBuilder("extra_required_field", LegacySQLTypeName.STRING)
                .setMode(Mode.REQUIRED).build())))
        .build());

    String input = Resources.getResource("testdata/json-payload.ndjson").getPath();
    String output = String.format("%s:%s", projectId, tableSpec);
    String errorOutput = outputPath + "/error/out";

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=text", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--output=" + output, "--errorOutputType=file",
        "--bqWriteMethod=streaming", "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutput=" + errorOutput });

    result.waitUntilFinish();

    assertTrue(stringValuesQuery("SELECT clientId FROM " + tableSpec).isEmpty());

    List<String> expectedErrorLines = Lines.resources("testdata/json-payload-wrapped.ndjson");
    List<String> errorOutputLines = Lines.files(outputPath + "/error/out*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(expectedErrorLines.size()));
  }

  private List<String> stringValuesQuery(String query) throws InterruptedException {
    return Lists.newArrayList(bigquery.create(JobInfo.of(QueryJobConfiguration.of(query)))
        .getQueryResults().iterateAll().iterator()).stream().map(fieldValues -> {
          FieldValue field = fieldValues.get(0);
          return field.isNull() ? null : field.getStringValue();
        }).collect(Collectors.toList());
  }

}
