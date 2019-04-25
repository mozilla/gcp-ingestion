# Cloud Function for loading Avro from GCS to BigQuery
Deployment is driven by Terraform [configuartion file](main.tf).

[Function](app/main.py) is triggered on object finalization (creation or update) in configured GCS bucket.
When this happens, it starts BigQuery load job that loads triggering file to corresponding table.

## Monitoring
Both function output and BigQuery load job logs are written to Stackdriver.


One of the most important checks here is verifying if all relevant files are loaded from GCS to BigQuery.
This can be implemented by querying Stackdriver load jobs' logs and comparing them to a list of files from GCS.
Stackdriver logs can be automatically exported to BigQuery which would offer more familiar interface for
validating state of the system.

## Testing
TBD, integration tests are easy to implement with provided Terraform configuration.

## [WiP] Miscellaneous Notes
We can load a file using:
```
bq load --source_format=AVRO --noreplace --job_id=users_avro test_from_avro.users gs://raw-avro-store-bucket/users.avro
```
Attempt to create a job with same id fails:
```
➜  avro-bq-loading-poc bq load --source_format=AVRO --noreplace --job_id=users_avro test_from_avro.users gs://raw-avro-store-bucket/users.avro
BigQuery error in load operation: Already Exists: Job dev-avro-bq-test-load:US.users_avro
```

We can list the jobs:
```
bq ls -j | awk 'NR <= 2 || $2=="load" {print $0}'
```

Load job fails if Avro schema does not match table schema:
```
➜  avro-bq-loading-poc bq load --source_format=AVRO --noreplace test_from_avro.users_new_table gs://raw-avro-store-bucket/users_new_schema.avro
Waiting on bqjob_r7c1b47db7df63317_0000016a49f65d8f_1 ... (0s) Current status: DONE   
BigQuery error in load operation: Error processing job 'dev-avro-bq-test-load:bqjob_r7c1b47db7df63317_0000016a49f65d8f_1': Provided Schema does not match Table
dev-avro-bq-test-load:test_from_avro.users_new_table. Cannot add fields (field: surname)
```


We can get load jobs' logs with:
```
gcloud logging read "logName=projects/dev-avro-bq-test-load/logs/cloudaudit.googleapis.com%2Fdata_access AND resource.type=bigquery_resource AND protoPayload.methodName=jobservice.jobcompleted"
```
And list ids of tagged jobs succeeded on a given day:
```
gcloud logging read "logName=projects/dev-avro-bq-test-load/logs/cloudaudit.googleapis.com%2Fdata_access AND resource.type=bigquery_resource AND protoPayload.methodName=jobservice.jobcompleted AND severity=INFO AND protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.labels.process=avro_loader timestamp>=\"2019-04-24T00:00:00.000Z\" AND timestamp<\"2019-04-25T00:00:00.000Z\"" --format json | jq '.[].protoPayload.serviceData.jobCompletedEvent.job.jobName.jobId'
```
(this could be more convenient if logs are exported to BigQuery)


We can create a metric to count errors:
```
gcloud beta logging metrics create load_jobs_error_codes --config-from-file=load_jobs_error_codes_metric.dat
gcloud beta logging metrics create load_jobs_completed --config-from-file=load_jobs_completed_metric.dat
```
And get them on a [dashboard](https://app.google.stackdriver.com/dashboards/2572502185107798913?project=dev-avro-bq-test-load&timeDomain=1h)


To quickly deploy and configure function:
```
terraform apply -auto-approve
```
And then to trigger:
```
gsutil cp users.avro gs://raw-avro-store-bucket/namespace/doctype/version/users-5.avro
```
