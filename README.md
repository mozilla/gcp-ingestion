# Telemetry Ingestion on Google Cloud Platform

[![CircleCI](https://circleci.com/gh/mozilla/gcp-ingestion.svg?style=svg&circle-token=d98a470269580907d5c6d74d0e67612834a21be7)](https://circleci.com/gh/mozilla/gcp-ingestion)

A monorepo for documentation and implementation of the Mozilla telemetry
ingestion system deployed to Google Cloud Platform (GCP).

There are currently four components:

- [ingestion-edge](ingestion-edge): a simple Python service for accepting HTTP
  messages and delivering to Google Cloud Pub/Sub
- [ingestion-beam](ingestion-beam): a Java module defining
  [Apache Beam](https://beam.apache.org/) jobs for streaming and batch
  transformations of ingested messages
- [ingestion-sink](ingestion-sink): a Java application that runs
  in Kubernetes, reading input from Google Cloud Pub/Sub and emitting
  records to outputs like GCS or BigQuery
- [ingestion-core](ingestion-core): a Java module for code shared between
  ingestion-beam and ingestion-sink

For more information, see [the documentation](https://mozilla.github.io/gcp-ingestion).

Java 11 support is a work in progress for the Beam Java SDK, so this project requires
Java 8 and will likely fail to compile using newer versions of the JDK.
To manage multiple local JDKs, consider [jenv](https://www.jenv.be/) and the
`jenv enable-plugin maven` command.
