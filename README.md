# Telemetry Ingestion on Google Cloud Platform

[![CircleCI](https://circleci.com/gh/mozilla/gcp-ingestion.svg?style=svg&circle-token=d98a470269580907d5c6d74d0e67612834a21be7)](https://circleci.com/gh/mozilla/gcp-ingestion)

A monorepo for documentation and implementation of the Mozilla telemetry
ingestion system deployed to Google Cloud Platform (GCP).

There are currently two components:

- [ingestion-edge](ingestion-edge): a simple Python service for accepting HTTP
  messages and delivering to Google Cloud Pub/Sub
- [ingestion-beam](ingestion-beam): a Java module defining
  [Apache Beam](https://beam.apache.org/) jobs for streaming and batch
  transformations of ingested messages

For more information, see [mozilla.github.io/gcp-ingestion](the documentation).
