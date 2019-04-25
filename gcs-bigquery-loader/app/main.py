from google.api_core.exceptions import Conflict
from google.cloud import bigquery
import os


def load_avro_to_bigquery(data, context):
    """Background Cloud Function triggered by Cloud Storage `finalize` events.
        Loads newly created Avro files to BigQuery.

        This function expects following directory structure on GCS:
        `{namespace}/{doc_type}/{version}/{file_name}.avro`
        
        Each file is appended to a corresponding table, its name constructed in the following way:
        `{namespace}_{doc_type}_{version}`
        Table needs to exist, otherwise the load job will fail and will not be retried. In such
        cases manual backfill will be required.

    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; asynchronous BigQuery load job is started, 
        the output is written to Stackdriver Logging
    """

    dataset = os.environ.get("BIGQUERY_DATASET", "default_dataset")

    bucket = data["bucket"]
    file_path = data["name"]

    print("Processing file:", file_path)

    namespace, doc_type, version, file_name = file_path.split("/")
    table_name = namespace + "_" + doc_type + "_" + version

    client = bigquery.Client()
    table_ref = client.dataset(dataset).table(table_name)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_NEVER
    job_config.source_format = bigquery.SourceFormat.AVRO

    # tagging helps with monitoring
    job_config.labels = {"process": "avro_loader"}

    uri = "gs://" + bucket + "/" + file_path
    job_id = "avro_loader_" + file_path.replace("/", "__").replace(".avro", "")
    try:
        load_job = client.load_table_from_uri(
            uri, table_ref, job_id=job_id, job_config=job_config
        )
        print("Starting job {}".format(load_job.job_id))
        return None
    except Conflict as c:
        print(
            "BigQuery responded with Conflict, which might be caused by duplicated "
            + "message or file rewrite. Exception is:",
            c.message,
        )
        return None
