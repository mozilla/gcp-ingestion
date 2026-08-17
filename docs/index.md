# GCP Ingestion

[GCP Ingestion](https://github.com/mozilla/gcp-ingestion/) is a monorepo for
documentation and implementation of the Mozilla telemetry ingestion system
deployed to Google Cloud Platform (GCP).

The components are:

- [ingestion-edge](ingestion-edge): a simple Python service for accepting HTTP
  messages and delivering to Google Cloud Pub/Sub
  ([deployment docs 🔒](https://mana.mozilla.org/wiki/display/SRE/Ingestion+Edge))
- [ingestion-beam](ingestion-beam): a Java module defining
  [Apache Beam](https://beam.apache.org/) jobs for streaming and batch
  transformations of ingested messages
  ([deployment docs 🔒](https://mana.mozilla.org/wiki/display/SRE/Ingestion+Beam))
- [ingestion-sink](ingestion-sink): a Java application that runs
  in Kubernetes, reading input from Google Cloud Pub/Sub and emitting
  records to outputs like GCS or BigQuery
  ([deployment docs 🔒](https://mana.mozilla.org/wiki/display/SRE/Ingestion+Sink))
- [ingestion-core](https://github.com/mozilla/gcp-ingestion/tree/main/ingestion-core): shared Java code used by both `ingestion-beam`
  and `ingestion-sink`; not a separately deployed service

The design behind the system along with various trade offs are documented in
the [architecture section](./architecture/overview.md).

This project requires Java 11.
To manage multiple local JDKs, consider [jenv](https://www.jenv.be/) and the
`jenv enable-plugin maven` command.
Also consider reading through
[Apache Beam's wiki article on IntelliJ IDEA setup](https://cwiki.apache.org/confluence/display/BEAM/Set+up+IntelliJ+from+scratch)
for some ideas on configuring an IDE environment.

`ingestion-core` is not consumed as a published jar; the root `pom.xml` co-compiles
its sources into `ingestion-beam` and `ingestion-sink` via `build-helper`'s
`add-source` goal. IDEs that don't recognize this report phantom "cannot resolve
symbol" errors - enable the opt-in `IDE` profile (in both module poms) to fix them.

Feel free to ask us in `#data-help` on Slack or `#telemetry` on `chat.mozilla.org`
if you have specific questions.
