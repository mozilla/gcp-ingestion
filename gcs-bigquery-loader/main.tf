provider "google" {
  credentials = "${file("dev-avro-bq-test-load-ca87beb49ed0.json")}"
  project     = "dev-avro-bq-test-load"
  region      = "us-west1"
}

resource "google_storage_bucket" "raw-avro-store" {
  name = "raw-avro-store-bucket"
}

resource "google_bigquery_dataset" "test-from-avro" {
  dataset_id = "test_from_avro"
}

locals {
  function_name    = "avro-to-bq"
  functions_region = "us-central1"

  // temporary workaround for https://github.com/terraform-providers/terraform-provider-google/issues/1938
  filename_on_gcs = "${local.function_name}-${lower(replace(base64encode(data.archive_file.function_dist.output_md5), "=", ""))}.zip"
}

resource "google_storage_bucket" "functions_store" {
  name = "avro-to-bq-prod-functions"
}

data "archive_file" "function_dist" {
  type        = "zip"
  source_dir  = "./app"
  output_path = "dist/${local.function_name}.zip"
}

resource "google_storage_bucket_object" "avro_to_bq_function_code" {
  name   = "${local.filename_on_gcs}.zip"
  bucket = "${google_storage_bucket.functions_store.name}"
  source = "${data.archive_file.function_dist.output_path}"
}

resource "google_project_service" "cloudfunctions-api" {
  project = "dev-avro-bq-test-load"
  service = "cloudfunctions.googleapis.com"

  disable_dependent_services = true
}

resource "google_cloudfunctions_function" "load-gcs-avro-to-bq" {
  name                  = "load-gcs-avro-to-bq"
  description           = "Triggered by changes to ${google_storage_bucket.raw-avro-store.name} bucket"
  available_memory_mb   = 128
  source_archive_bucket = "${google_storage_bucket_object.avro_to_bq_function_code.bucket}"
  source_archive_object = "${google_storage_bucket_object.avro_to_bq_function_code.name}"
  entry_point           = "load_avro_to_bigquery"
  timeout               = 60
  runtime               = "python37"

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = "${google_storage_bucket.raw-avro-store.name}"

    failure_policy {
      # this can be flipped if function catched all non-transient errors
      # see also: https://cloud.google.com/functions/docs/bestpractices/retries#set_an_end_condition_to_avoid_infinite_retry_loops
      retry = false
    }
  }

  labels {
    process = "avro-loader"
  }

  environment_variables = {
    BIGQUERY_DATASET = "test_from_avro"
  }

  region = "${local.functions_region}"
}
