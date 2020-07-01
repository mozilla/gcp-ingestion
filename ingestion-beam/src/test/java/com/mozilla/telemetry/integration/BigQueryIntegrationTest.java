package com.mozilla.telemetry.integration;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Clustering;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.mozilla.telemetry.Sink;
import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class BigQueryIntegrationTest extends TestWithDeterministicJson {

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

  private static final TimePartitioning TIME_PARTITIONING = TimePartitioning.newBuilder(Type.DAY)
      .setField("submission_timestamp").build();

  private static final Clustering CLUSTERING = Clustering.newBuilder()
      .setFields(ImmutableList.of("submission_timestamp")).build();

  @Test
  public void canWriteToBigQuery() throws Exception {
    String table = "my_test_table";
    String tableSpec = String.format("%s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery
        .create(TableInfo
            .newBuilder(tableId,
                StandardTableDefinition
                    .of(Schema.of(Field.of("client_id", LegacySQLTypeName.STRING),
                        Field.of("type", LegacySQLTypeName.STRING),
                        Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP)))
                    .toBuilder().setTimePartitioning(TIME_PARTITIONING).setClustering(CLUSTERING)
                    .build())
            .build());

    String input = Resources
        .getResource("testdata/bigquery-integration/input-with-attributes.ndjson").getPath();
    String output = String.format("%s:%s", projectId, tableSpec);

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--bqWriteMethod=file_loads",
        "--tempLocation=gs://gcp-ingestion-static-test-bucket/temp/bq-loads",
        "--schemasLocation=schemas.tar.gz", "--output=" + output, "--errorOutputType=stderr" });

    result.waitUntilFinish();

    assertThat(stringValuesQueryWithRetries("SELECT submission_timestamp FROM " + tableSpec),
        matchesInAnyOrder(Lists.newArrayList(null, null, "1561983194.123456")));
    assertThat(stringValuesQueryWithRetries("SELECT client_id FROM " + tableSpec),
        matchesInAnyOrder(ImmutableList.of("abc123", "abc123", "def456")));
  }

  @Test
  public void canWriteToDynamicTables() throws Exception {
    String table = "my_test_table";
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery.create(TableInfo.newBuilder(tableId,
        StandardTableDefinition.of(Schema.of(Field.of("client_id", LegacySQLTypeName.STRING),
            Field.of("type", LegacySQLTypeName.STRING))))
        .build());

    String input = Resources
        .getResource("testdata/bigquery-integration/input-varied-doctypes.ndjson").getPath();
    String output = String.format("%s:%s.%s", projectId, dataset, "${document_type}_table");
    String errorOutput = outputPath + "/error/out";

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--output=" + output,
        "--bqWriteMethod=streaming", "--errorOutputType=file", "--schemasLocation=schemas.tar.gz",
        "--errorOutputFileCompression=UNCOMPRESSED", "--errorOutput=" + errorOutput });

    result.waitUntilFinish();

    String tableSpec = String.format("%s.%s", dataset, table);
    assertThat(stringValuesQueryWithRetries("SELECT client_id FROM " + tableSpec),
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
        .create(TableInfo
            .newBuilder(tableId,
                StandardTableDefinition
                    .of(Schema.of(Field.of("client_id", LegacySQLTypeName.STRING),
                        Field.of("type", LegacySQLTypeName.STRING),
                        Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP)))
                    .toBuilder().setTimePartitioning(TIME_PARTITIONING).setClustering(CLUSTERING)
                    .build())
            .build());

    String input = Resources
        .getResource("testdata/bigquery-integration/input-varied-doctypes.ndjson").getPath();
    String output = String.format("%s:%s.%s", projectId, dataset, "${document_type}_table");
    String errorOutput = outputPath + "/error/out";

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--output=" + output,
        "--bqWriteMethod=file_loads", "--errorOutputType=file",
        "--tempLocation=gs://gcp-ingestion-static-test-bucket/temp/bq-loads",
        "--schemasLocation=schemas.tar.gz", "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutput=" + errorOutput });

    result.waitUntilFinish();

    String tableSpec = String.format("%s.%s", dataset, table);
    assertThat(stringValuesQueryWithRetries("SELECT client_id FROM " + tableSpec),
        matchesInAnyOrder(ImmutableList.of("abc123")));

    List<String> errorOutputLines = Lines.files(outputPath + "/error/out*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(2));
  }

  private void canWriteWithMixedMethod(String streamingDocTypes) throws Exception {
    String table = "my_test_table";
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery
        .create(TableInfo
            .newBuilder(tableId,
                StandardTableDefinition
                    .of(Schema.of(Field.of("client_id", LegacySQLTypeName.STRING),
                        Field.of("type", LegacySQLTypeName.STRING),
                        Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP)))
                    .toBuilder().setTimePartitioning(TIME_PARTITIONING).setClustering(CLUSTERING)
                    .build())
            .build());

    String input = Resources
        .getResource("testdata/bigquery-integration/input-varied-doctypes.ndjson").getPath();
    String output = String.format("%s:%s.%s", projectId, dataset, "${document_type}_table");
    String errorOutput = outputPath + "/error/out";

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--output=" + output, "--bqWriteMethod=mixed",
        "--bqStreamingDocTypes=" + streamingDocTypes, "--errorOutputType=file",
        "--tempLocation=gs://gcp-ingestion-static-test-bucket/temp/bq-loads",
        "--schemasLocation=schemas.tar.gz", "--errorOutputFileCompression=UNCOMPRESSED",
        "--errorOutput=" + errorOutput });

    result.waitUntilFinish();

    String tableSpec = String.format("%s.%s", dataset, table);
    assertThat(stringValuesQueryWithRetries("SELECT client_id FROM " + tableSpec),
        matchesInAnyOrder(ImmutableList.of("abc123")));

    List<String> errorOutputLines = Lines.files(outputPath + "/error/out*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(2));
  }

  @Test
  public void canWriteWithMixedMethodStreamingOneDocType() throws Exception {
    canWriteWithMixedMethod("my-namespace/my-test");
  }

  @Test
  public void canWriteWithMixedMethodStreamingAllDocTypes() throws Exception {
    canWriteWithMixedMethod("*");
  }

  @Test
  public void canRecoverFailedInsertsInStreamingMode() throws Exception {
    String table = "my_test_table";
    String tableSpec = String.format("%s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());

    bigquery.create(TableInfo.newBuilder(tableId,
        StandardTableDefinition.of(Schema.of(Field.of("client_id", LegacySQLTypeName.STRING),
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

    assertTrue(stringValuesQuery("SELECT client_id FROM " + tableSpec).isEmpty());

    List<String> expectedErrorLines = Lines.resources("testdata/json-payload-wrapped.ndjson");
    List<String> errorOutputLines = Lines.files(outputPath + "/error/out*.ndjson");
    assertThat(errorOutputLines, Matchers.hasSize(expectedErrorLines.size()));
  }

  @Test
  public void canSetStrictSchemaMode() throws Exception {
    String table = "my_test_table";
    String tableSpec = String.format("%s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery
        .create(TableInfo
            .newBuilder(tableId,
                StandardTableDefinition
                    .of(Schema.of(Field.of("client_id", LegacySQLTypeName.STRING),
                        Field.of("additional_properties", LegacySQLTypeName.STRING),
                        Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP)))
                    .toBuilder().setTimePartitioning(TIME_PARTITIONING).setClustering(CLUSTERING)
                    .build())
            .build());

    String input = Resources
        .getResource("testdata/bigquery-integration/input-varied-doctypes.ndjson").getPath();
    String output = String.format("%s:%s", projectId, tableSpec);

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--bqWriteMethod=streaming",
        "--bqStrictSchemaDocTypes=my-namespace/my-test", "--output=" + output,
        "--errorOutputType=stderr" });

    result.waitUntilFinish();

    assertThat(stringValuesQueryWithRetries("SELECT additional_properties FROM " + tableSpec),
        matchesInAnyOrder(Lists.newArrayList("{\"type\":\"main\"}", null, "{\"type\":\"main\"}")));
  }

  @Test
  public void canLoadRawFormat() throws Exception {
    String table = "my_test_table";
    String tableSpec = String.format("%s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery
        .create(TableInfo
            .newBuilder(tableId,
                StandardTableDefinition
                    .of(Schema.of(Field.of("args", LegacySQLTypeName.STRING),
                        Field.of("content_length", LegacySQLTypeName.STRING),
                        Field.of("date", LegacySQLTypeName.STRING),
                        Field.of("dnt", LegacySQLTypeName.STRING),
                        Field.of("host", LegacySQLTypeName.STRING),
                        Field.of("method", LegacySQLTypeName.STRING),
                        Field.of("payload", LegacySQLTypeName.BYTES),
                        Field.of("protocol", LegacySQLTypeName.STRING),
                        Field.of("remote_addr", LegacySQLTypeName.STRING),
                        Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP),
                        Field.of("uri", LegacySQLTypeName.STRING),
                        Field.of("user_agent", LegacySQLTypeName.STRING),
                        Field.of("x_debug_id", LegacySQLTypeName.STRING),
                        Field.of("x_forwarded_for", LegacySQLTypeName.STRING),
                        Field.of("x_pingsender_version", LegacySQLTypeName.STRING),
                        Field.of("x_pipeline_proxy", LegacySQLTypeName.STRING)))
                    .toBuilder().setTimePartitioning(TIME_PARTITIONING).setClustering(CLUSTERING)
                    .build())
            .build());

    String input = Resources.getResource("testdata/bigquery-integration/input-raw-format.ndjson")
        .getPath();
    String output = String.format("%s:%s", projectId, tableSpec);

    PipelineResult result = Sink
        .run(new String[] { "--inputFileFormat=json", "--inputType=file", "--input=" + input,
            "--outputType=bigquery", "--bqWriteMethod=streaming", "--decompressInputPayloads=false",
            "--outputTableRowFormat=raw", "--output=" + output, "--errorOutputType=stderr" });

    result.waitUntilFinish();

    assertEquals(
        Json.asString(ImmutableMap.<String, String>builder()
            .put("submission_timestamp", "2020-01-12T21:02:18.123456Z")
            .put("uri",
                "/submit/telemetry/6c49ec73-4350-45a0-9c8a-6c8f5aded0cf/main/Firefox/58.0.2"
                    + "/release/20180206200532")
            .put("protocol", "HTTP/1.1").put("method", "POST").put("args", "v=4")
            .put("remote_addr", "172.31.32.5").put("content_length", "4722")
            .put("date", "Mon, 12 Jan 2020 21:02:18 GMT").put("dnt", "1")
            .put("host", "incoming.telemetry.mozilla.org").put("user_agent", "pingsender/1.0")
            .put("x_forwarded_for", "10.98.132.74, 103.3.237.12").put("x_pingsender_version", "1.0")
            .put("x_debug_id", "my_debug_session_1")
            .put("x_pipeline_proxy", "2020-01-12T21:02:18.123456Z")
            // ensure this value stayed compressed when loaded
            .put("payload", "H4sIAJBj8l4AAytJLS4BAAx+f9gEAAAA").build()),
        String.join("\n",
            stringValuesQueryWithRetries("SELECT TO_JSON_STRING(t) FROM " + tableSpec + " AS t")));
  }

  @Test
  public void canLoadAndReadDecodedFormat() throws Exception {
    String table = "my_test_table";
    String tableSpec = String.format("%s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery.create(TableInfo.newBuilder(tableId, StandardTableDefinition
        .of(Schema.of(Field.of("document_id", LegacySQLTypeName.STRING), Field.of("metadata",
            LegacySQLTypeName.RECORD, Field.of("document_namespace", LegacySQLTypeName.STRING),
            Field.of("document_type", LegacySQLTypeName.STRING),
            Field.of("document_version", LegacySQLTypeName.STRING),
            Field.of("geo", LegacySQLTypeName.RECORD, Field.of("city", LegacySQLTypeName.STRING),
                Field.of("country", LegacySQLTypeName.STRING),
                Field.of("subdivision1", LegacySQLTypeName.STRING),
                Field.of("subdivision2", LegacySQLTypeName.STRING)),
            Field.of("header", LegacySQLTypeName.RECORD, Field.of("date", LegacySQLTypeName.STRING),
                Field.of("dnt", LegacySQLTypeName.STRING),
                Field.of("x_debug_id", LegacySQLTypeName.STRING),
                Field.of("x_pingsender_version", LegacySQLTypeName.STRING)),
            Field.of("uri", LegacySQLTypeName.RECORD,
                Field.of("app_build_id", LegacySQLTypeName.STRING),
                Field.of("app_name", LegacySQLTypeName.STRING),
                Field.of("app_update_channel", LegacySQLTypeName.STRING),
                Field.of("app_version", LegacySQLTypeName.STRING)),
            Field.of("user_agent", LegacySQLTypeName.RECORD,
                Field.of("browser", LegacySQLTypeName.STRING),
                Field.of("browser_version", LegacySQLTypeName.STRING),
                Field.of("os", LegacySQLTypeName.STRING),
                Field.of("os_version", LegacySQLTypeName.STRING))),
            Field.of("normalized_app_name", LegacySQLTypeName.STRING),
            Field.of("normalized_channel", LegacySQLTypeName.STRING),
            Field.of("normalized_country_code", LegacySQLTypeName.STRING),
            Field.of("normalized_os", LegacySQLTypeName.STRING),
            Field.of("normalized_os_version", LegacySQLTypeName.STRING),
            Field.of("payload", LegacySQLTypeName.BYTES),
            Field.of("sample_id", LegacySQLTypeName.INTEGER),
            Field.of("submission_timestamp", LegacySQLTypeName.TIMESTAMP)))
        .toBuilder().setTimePartitioning(TIME_PARTITIONING).setClustering(CLUSTERING).build())
        .build());

    String input = Resources
        .getResource("testdata/bigquery-integration/input-decoded-format.ndjson").getPath();
    String fullyQualifiedTableSpec = String.format("%s:%s", projectId, tableSpec);

    PipelineResult result = Sink.run(new String[] { "--inputFileFormat=json", "--inputType=file",
        "--input=" + input, "--outputType=bigquery", "--bqWriteMethod=file_loads",
        "--tempLocation=gs://gcp-ingestion-static-test-bucket/temp/bq-loads",
        "--decompressInputPayloads=false", "--outputTableRowFormat=decoded",
        "--output=" + fullyQualifiedTableSpec, "--errorOutputType=stderr" });

    result.waitUntilFinish();

    Map<String, Object> expectedMap = ImmutableMap.<String, Object>builder()
        .put("document_id", "6c49ec73-4350-45a0-9c8a-6c8f5aded0cf")
        .put("metadata", ImmutableMap.<String, Object>builder()
            .put("document_namespace", "telemetry").put("document_version", "4")
            .put("document_type", "main")
            .put("geo",
                ImmutableMap
                    .<String, String>builder().put("country", "US").put("subdivision1", "WA")
                    .put("subdivision2", "Clark").put("city", "Vancouver").build())
            .put("header",
                ImmutableMap.<String, String>builder().put("date", "Mon, 12 Jan 2020 21:02:18 GMT")
                    .put("dnt", "1").put("x_pingsender_version", "1.0")
                    .put("x_debug_id", "my_debug_session_1").build())
            .put("uri",
                ImmutableMap.<String, String>builder().put("app_name", "Firefox")
                    .put("app_version", "58.0.2").put("app_update_channel", "release")
                    .put("app_build_id", "20180206200532").build())
            .put("user_agent", ImmutableMap.<String, String>builder().put("browser", "pingsender")
                .put("browser_version", "1.0").put("os", "Windows").put("os_version", "10").build())
            .build())
        .put("submission_timestamp", "2020-01-12T21:02:18.123456Z")
        .put("normalized_app_name", "Firefox").put("normalized_country_code", "US")
        .put("normalized_os", "Windows").put("normalized_os_version", "10").put("sample_id", 42)
        // ensure this value stayed compressed when loaded
        .put("payload", "H4sIAJBj8l4AAytJLS4BAAx+f9gEAAAA").build();
    expectedMap = new HashMap<>(expectedMap);
    expectedMap.put("normalized_channel", null);
    assertEquals(Json.asString(expectedMap), String.join("\n",
        stringValuesQueryWithRetries("SELECT TO_JSON_STRING(t) FROM " + tableSpec + " AS t")));

    String fileOutput = outputPath + "/out";
    PipelineResult readResult = Sink.run(new String[] { "--inputType=bigquery_table",
        "--input=" + fullyQualifiedTableSpec, "--project=" + projectId, "--bqReadMethod=storageapi",
        "--outputFileCompression=UNCOMPRESSED",
        "--bqSelectedFields=payload,normalized_channel,normalized_app_name,submission_timestamp",
        "--bqRowRestriction=CAST(submission_timestamp AS DATE)"
            + " BETWEEN '2020-01-10' AND '2020-01-14'",
        "--outputType=file", "--outputFileFormat=json", "--output=" + fileOutput,
        "--errorOutputType=stderr" });

    readResult.waitUntilFinish();

    List<String> outputLines = Lines.files(fileOutput + "*.ndjson");
    assertEquals(1, outputLines.size());
    assertEquals("Output read from BigQuery differed from expectation",
        Json.asString(ImmutableMap.of("attributeMap",
            ImmutableMap.of("normalized_app_name", "Firefox", //
                "submission_timestamp", "2020-01-12T21:02:18.123456Z"),
            // ensure this value was decompressed when read
            "payload", "dGVzdA==")),
        outputLines.get(0));
  }

  private List<String> stringValuesQuery(String query) throws InterruptedException {
    return Lists.newArrayList(bigquery.create(JobInfo.of(QueryJobConfiguration.of(query)))
        .getQueryResults().iterateAll().iterator()).stream().map(fieldValues -> {
          FieldValue field = fieldValues.get(0);
          return field.isNull() ? null : field.getStringValue();
        }).collect(Collectors.toList());
  }

  private List<String> stringValuesQueryWithRetries(String query) throws InterruptedException {
    for (int i = 0; i < 2; i++) {
      List<String> result = stringValuesQuery(query);
      if (!result.isEmpty()) {
        return result;
      }
      Thread.sleep(100);
    }
    throw new RuntimeException("Query returned unexpected empty result after 5 attempts");
  }

}
