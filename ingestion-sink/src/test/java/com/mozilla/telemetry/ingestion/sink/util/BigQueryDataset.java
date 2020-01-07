package com.mozilla.telemetry.ingestion.sink.util;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * JUnit Rule for reusing code to interact with BigQuery.
 */
public class BigQueryDataset extends TestWatcher {

  public BigQuery bigquery;
  public String project;
  public String dataset;

  /** Find credentials in the environment and create a dataset in BigQuery. */
  @Override
  protected void starting(Description description) {
    RemoteBigQueryHelper bqHelper = RemoteBigQueryHelper.create();
    bigquery = bqHelper.getOptions().getService();
    project = bqHelper.getOptions().getProjectId();
    dataset = RemoteBigQueryHelper.generateDatasetName();
    bigquery.create(DatasetInfo.newBuilder(dataset).build());
  }

  /** Remove all resources we created in BigQuery. */
  @Override
  protected void finished(Description description) {
    RemoteBigQueryHelper.forceDelete(bigquery, dataset);
  }
}
