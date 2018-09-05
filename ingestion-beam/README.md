[![CircleCI](https://circleci.com/gh/mozilla/gcp-ingestion.svg?style=svg&circle-token=d98a470269580907d5c6d74d0e67612834a21be7)](https://circleci.com/gh/mozilla/gcp-ingestion)

# Apache Beam Jobs for Ingestion

This java module contains our Apache Beam jobs for use in Ingestion.
Google Cloud Dataflow is a Google Cloud Platform service that natively runs
Apache Beam jobs.

# Table of Contents

 * [Sink Job](#sink-job)
   * [Supported Input and Outputs](#supported-input-and-outputs)
   * [Encoding](#encoding)
   * [Executing Jobs](#executing-jobs)
 * [Validation Job](#validation-job)
   * [Transforms](#transforms)
   * [Executing Validation Jobs](#executing-validation-jobs)
 * [Testing](#testing)

# Sink Job

A job for delivering messages between Google Cloud services.

## Supported Input and Outputs

Supported inputs:

 * Google Cloud PubSub
 * Google Cloud Storage

Supported outputs:

 * Google Cloud PubSub
 * Google Cloud Storage
 * Google Cloud BigQuery
 * stdout
 * stderr

Supported error outputs, must include attributes and must not validate messages:

 * Google Cloud PubSub
 * Google Cloud Storage with JSON encoding
 * stdout with JSON encoding
 * stderr with JSON encoding

## Encoding

Internally messages are stored and transported as
[PubsubMessage](https://beam.apache.org/documentation/sdks/javadoc/2.6.0/org/apache/beam/sdk/io/gcp/pubsub/PubsubMessage.html).

Supported file formats for Cloud Storage are `json` or `text`. The `json` file
format stores newline delimited JSON, encoding the field `payload` as a base64
string, and `attributeMap` as an optional object with string keys and values.
The `text` file format stores newline delimited strings, encoding the field
`payload` as `UTF-8`.

We'll construct example inputs based on the following two values and their base64 encodings:

```
$ echo -en "test" | base64
dGVzdA==

$ echo -en "test\n" | base64
dGVzdAo=
```

Example `json` file:

    {"payload":"dGVzdA==","attributeMap":{"meta":"data"}}
    {"payload":"dGVzdAo=","attributeMap":null}
    {"payload":"dGVzdA=="}

The above file when stored in the `text` format:

    test
    test

    test

Note that the newline embedded at the end of the second JSON message results in
two text messages, one of which is blank.

## Executing Jobs

Note: `-Dexec.args` does not handle newlines gracefully, but bash will remove
`\` escaped newlines in `"`s.

### Locally

If you install Java and maven, you can invoke `mvn` directly in the following commands;
be aware, though, that Java 8 is the target JVM and some reflection warnings may be thrown on
newer versions, though these are generally harmless.

The provided `bin/mvn` script downloads and runs maven via docker so that less
setup is needed on the local machine.

```bash
# create a test input file
mkdir -p tmp/
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' > tmp/input.json

# consume messages from the test file, decode and re-encode them, and write to a directory
./bin/mvn compile exec:java -Dexec.args="\
    --inputFileFormat=json \
    --inputType=file \
    --input=tmp/input.json \
    --outputFileFormat=json \
    --outputType=file \
    --output=tmp/output/out \
    --errorOutputType=file \
    --errorOutput=tmp/error \
"

# check that the message was delivered
cat tmp/output/*

# write message payload straight to stdout
./bin/mvn compile exec:java -Dexec.args="\
    --inputFileFormat=json \
    --inputType=file \
    --input=tmp/input.json \
    --outputFileFormat=text \
    --outputType=stdout \
    --errorOutputType=stderr \
"

# check the help page to see all options
./bin/mvn compile exec:java -Dexec.args=--help=Options
```

### On Dataflow

```bash
# Pick a bucket to store files in
BUCKET="gs://$(gcloud config get-value project)"

# create a test input file
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' | gsutil cp - $BUCKET/input.json

# consume messages from the test file, decode and re-encode them, and write to a bucket
./bin/mvn compile exec:java -Dexec.args="\
    --runner=Dataflow \
    --inputFileFormat=json \
    --inputType=file \
    --input=$BUCKET/input.json \
    --outputFileFormat=json \
    --outputType=file \
    --output=$BUCKET/output \
    --errorOutputType=file \
    --errorOutput=$BUCKET/error \
"

# wait for the job to finish
gcloud dataflow jobs list

# check that the message was delivered
gsutil cat $BUCKET/output/*
```

### On Dataflow with templates

```bash
# Pick a bucket to store files in
BUCKET="gs://$(gcloud config get-value project)"

# create a template
./bin/mvn compile exec:java -Dexec.args="\
    --runner=Dataflow \
    --inputFileFormat=json \
    --inputType=file \
    --outputFileFormat=json \
    --outputType=file \
    --errorOutputType=file \
    --templateLocation=$BUCKET/sink/templates/JsonFileToJsonFile \
    --stagingLocation=$BUCKET/sink/staging \
"

# create a test input file
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' | gsutil cp - $BUCKET/input.json

# run the dataflow template with gcloud
JOBNAME=FileToFile1
gcloud dataflow jobs run $JOBNAME --gcs-location=$BUCKET/sink/templates/JsonFileToJsonFile --parameters "input=$BUCKET/input.json,output=$BUCKET/output/,errorOutput=$BUCKET/error"

# get the job id
JOB_ID="$(gcloud dataflow jobs list --filter name=fileToStdout1 | tail -1 | cut -d' ' -f1)"

# wait for the job to finish
gcloud dataflow jobs show "$JOB_ID"

# check that the message was delivered
gsutil cat $BUCKET/output/*
```

# Validation Job

A job for normalizing ingestion messages.

## Transforms

These transforms are currently executed against each message in order.

### GeoIP Lookup

1. Extract `ip` from the `x_forwarded_for` attribute
   * fall back to the `remote_addr` attribute, then to an empty string
1. Execute the following steps until one fails and ignore the exception
    1. Parse `ip` using `InetAddress.getByName`
    1. Lookup `ip` in the configured `GeoIP2City.mmdb`
    1. Extract `country.iso_code` as `geo_country`
    1. Extract `city.name` as `geo_city`
    1. Extract `subdivisions[0].iso_code` as `geo_subdivision1`
    1. Extract `subdivisions[1].iso_code` as `geo_subdivision2`
1. Remove the `x_forwarded_for` and `remote_addr` attributes
1. Remove any`null` values added to attributes

## Executing Validation Jobs

Validation jobs are executed the same way as [executing sink jobs](#executing-jobs)
but with a few extra flags:

 * `-Dexec.mainClass=com.mozilla.telemetry.Validate`
 * `--geoCityDatabase=/path/to/GeoIP2-City.mmdb`

Example:

```bash
# create a test input file
mkdir -p tmp/
echo '{"payload":"dGVzdA==","attributeMap":{"remote_addr":"63.245.208.195"}}' > tmp/input.json

# Download `GeoLite2-City.mmdb`
./bin/download-geolite2

# do geo lookup on messages to stdout
./bin/mvn compile exec:java -Dexec.mainClass=com.mozilla.telemetry.Validate -Dexec.args="\
    --geoCityDatabase=GeoLite2-City.mmdb \
    --inputType=file \
    --input=tmp/input.json \
    --outputType=stdout \
    --errorOutputType=stderr \
"
```

# Testing

Run tests locally with [CircleCI Local CLI](https://circleci.com/docs/2.0/local-cli/#installing-the-circleci-local-cli-on-macos-and-linux-distros)

```bash
(cd .. && circleci build --job ingestion-beam)
```

To make more targeted test invocations, you can install Java and maven locally or
use the `bin/mvn` executable to run maven in docker:

```bash
./bin/mvn clean test
```

# License

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
