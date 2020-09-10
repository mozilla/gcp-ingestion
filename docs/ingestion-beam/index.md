# Apache Beam Jobs for Ingestion

This ingestion-beam java module contains our [Apache Beam](https://beam.apache.org/) jobs for use in Ingestion.
Google Cloud Dataflow is a Google Cloud Platform service that natively runs
Apache Beam jobs.

The source code lives in the [ingestion-beam](https://github.com/mozilla/gcp-ingestion/tree/master/ingestion-beam)
subdirectory of the gcp-ingestion repository.

There are currently three jobs defined, please see the respective sections on them in the
documentation:

- [Sink job](./sink-job.md): A job for delivering messages between Google Cloud services
  - deprecated in favor of [ingestion-sink](../ingestion-sink)
- [Decoder job](./decoder-job.md): A job for normalizing ingestion messages
- [Republisher job](./republisher-job.md): A job for republishing subsets of decoded messages to new destinations

## Building

Move to the `ingestion-beam` subdirectory of your gcp-ingestion checkout and run:

```bash
./bin/mvn clean compile
```

See the details below under each job for details on how to run what you've produced.

## Testing

Before anything else, be sure to download the test data:

```bash
./bin/download-cities15000
./bin/download-geolite2
./bin/download-schemas
```

Run tests locally with [CircleCI Local CLI](https://circleci.com/docs/2.0/local-cli/#installing-the-circleci-local-cli-on-macos-and-linux-distros)

```bash
(cd .. && circleci build --job ingestion-beam)
```

To make more targeted test invocations, you can install Java and maven locally or
use the `bin/mvn` executable to run maven in docker:

```bash
./bin/mvn clean test
```

If you wish to just run a single test class or a single test case, try something like this:

```bash
# Run all tests in a single class
./bin/mvn test -Dtest=com.mozilla.telemetry.util.SnakeCaseTest

# Run only a single test case
./bin/mvn test -Dtest='com.mozilla.telemetry.util.SnakeCaseTest#testSnakeCaseFormat'
```

To run the project in a sandbox against production data, see this document on
[configuring an integration testing workflow](./ingestion_testing_workflow.md).

## Code Formatting

Use spotless to automatically reformat code:

```bash
mvn spotless:apply
```

or just check what changes it requires:

```bash
mvn spotless:check
```
