# GCP Ingestion

[GCP Ingestion](https://github.com/mozilla/gcp-ingestion/) is a monorepo for
documentation and implementation of the Mozilla telemetry ingestion system
deployed to Google Cloud Platform (GCP).

There are currently two components:

- [ingestion-edge](./ingestion-edge/index.md): a simple Python service for accepting HTTP
  messages and delivering to Google Cloud Pub/Sub
- [ingestion-beam](ingestion-beam): a Java module defining
  [Apache Beam](https://beam.apache.org/) jobs for streaming and batch
  transformations of ingested messages

The design behind the system along with various trade offs are documented in
the architecture section. Note that as of this writing (August 2019) this
GCP ingestion is changing quickly, so some parts of this documentation may be out
of date.

Feel free to ask us on irc.mozilla.org #datapipeline or #fx-metrics
on slack if you have specific questions.
