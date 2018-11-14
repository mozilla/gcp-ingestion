/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import static com.mozilla.telemetry.matchers.Lines.matchesInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite that accesses BigQuery in GCP.
 *
 * <p>Because this requires credentials, this suite is excluded by default in the surefire
 * configuration, but can be enabled by passing command-line option
 * {@code -Dtest=BigQueryIntegrationTest}. Credentials can be provided by initializing a
 * configuration in the gcloud command-line tool or by providing a path to service account
 * credentials in environment variable {@code GOOGLE_APPLICATION_CREDENTIALS}.
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

  /** Remove all resources we created in BigQuery. */
  @After
  public void deleteBigQueryDataset() {
    RemoteBigQueryHelper.forceDelete(bigquery, dataset);
  }

  @Test
  public void canWriteToBigQuery() throws Exception {
    String table = "mytable";
    String tableSpec = String.format("%s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, table);

    bigquery.create(DatasetInfo.newBuilder(dataset).build());
    bigquery.create(TableInfo.newBuilder(tableId,
        StandardTableDefinition.of(Schema.of(Field.of("clientId", LegacySQLTypeName.STRING),
            Field.of("type", LegacySQLTypeName.STRING))))
        .build());

    String input = Resources.getResource("testdata/json-payload.ndjson").getPath();
    String output = String.format("%s:%s", projectId, tableSpec);

    Sink.main(new String[] { "--inputFileFormat=text", "--inputType=file", "--input=" + input,
        "--outputFileFormat=text", "--outputType=bigquery", "--output=" + output,
        "--errorOutputType=stderr" });

    assertThat(stringValuesQuery("SELECT clientId FROM " + tableSpec),
        matchesInAnyOrder(ImmutableList.of("abc123", "abc123", "def456")));
  }

  private List<String> stringValuesQuery(String query) throws InterruptedException {
    return Lists
        .newArrayList(bigquery.create(JobInfo.of(QueryJobConfiguration.of(query))).getQueryResults()
            .iterateAll().iterator())
        .stream().map(fieldValues -> fieldValues.get(0).getStringValue())
        .collect(Collectors.toList());
  }

}
