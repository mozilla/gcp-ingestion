# Telemetry Ingestion on Google Cloud Platform

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!-- END doctoc generated TOC please keep comment here to allow auto update -->

A monorepo for documentation and implementation of the Mozilla telemetry
ingestion system deployed to Google Cloud Platform (GCP).

The overall architecture is described in [docs/architecture](docs/architecture)
along with commentary on design decisions.
Individual components are specified under [docs](docs) and implemented
under the various `ingestion-*` service directories:

- [ingestion-edge](ingestion-edge): a simple Python service for accepting HTTP
  messages and delivering to Google Cloud Pub/Sub
- [ingestion-beam](ingestion-beam): a Java module defining
  [Apache Beam](https://beam.apache.org/) jobs for streaming and batch
  transformations of ingested messages
