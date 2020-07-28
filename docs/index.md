# GCP Ingestion

[GCP Ingestion](https://github.com/mozilla/gcp-ingestion/) is a monorepo for
documentation and implementation of the Mozilla telemetry ingestion system
deployed to Google Cloud Platform (GCP).

The components are:

- [ingestion-edge](ingestion-edge): a simple Python service for accepting HTTP
  messages and delivering to Google Cloud Pub/Sub
- [ingestion-beam](ingestion-beam): a Java module defining
  [Apache Beam](https://beam.apache.org/) jobs for streaming and batch
  transformations of ingested messages
- [ingestion-sink](ingestion-sink): a Java application that runs
  in Kubernetes, reading input from Google Cloud Pub/Sub and emitting
  records to batch-oriented outputs like GCS or BigQuery

The design behind the system along with various trade offs are documented in
the [architecture section](./architecture/overview.md).

Feel free to ask us in `#fx-metrics` on Slack or `#telemetry` on `chat.mozilla.org`
if you have specific questions.
