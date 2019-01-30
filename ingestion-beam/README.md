[![CircleCI](https://circleci.com/gh/mozilla/gcp-ingestion.svg?style=svg&circle-token=d98a470269580907d5c6d74d0e67612834a21be7)](https://circleci.com/gh/mozilla/gcp-ingestion)

# Apache Beam Jobs for Ingestion

This java module contains our Apache Beam jobs for use in Ingestion.
Google Cloud Dataflow is a Google Cloud Platform service that natively runs
Apache Beam jobs.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Code Formatting](#code-formatting)
- [Sink Job](#sink-job)
  - [Supported Input and Outputs](#supported-input-and-outputs)
  - [Encoding](#encoding)
  - [Output Path Specification](#output-path-specification)
    - [BigQuery](#bigquery)
    - [Protocol](#protocol)
    - [Attribute placeholders](#attribute-placeholders)
    - [File prefix](#file-prefix)
  - [Executing Jobs](#executing-jobs)
    - [Locally](#locally)
    - [On Dataflow](#on-dataflow)
    - [On Dataflow with templates](#on-dataflow-with-templates)
    - [In streaming mode](#in-streaming-mode)
- [Decoder Job](#decoder-job)
  - [Transforms](#transforms)
    - [Parse URI](#parse-uri)
    - [Decompress](#decompress)
    - [GeoIP Lookup](#geoip-lookup)
    - [Parse User Agent](#parse-user-agent)
  - [Executing Decoder Jobs](#executing-decoder-jobs)
- [Testing](#testing)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Code Formatting

Use spotless to automatically reformat code:

```bash
mvn spotless:apply
```

or use just check what changes it requires:

```bash
mvn spotless:check
```

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

## Output Path Specification

Depending on the specified output type, the `--output` path that you provide controls
several aspects of the behavior.

### BigQuery

When `--outputType=bigquery`, `--output` is a `tableSpec` of form `dataset.tablename`
or the more verbose `projectId:dataset.tablename`. The values can contain
attribute placeholders of form `${attribute_name}`. To set dataset to the
document namespace and table name to the document type, specify:

    --output='${document_namespace}.${document_type}'

All `-` characters in the attributes will be converted to `_` per BigQuery
naming restrictions.
Defaults for the placeholders using `${attribute_name:-default_value}`
are supported, but likely don't make much sense since it's unlikely that
there is a default table whose schema is compatible with all potential
payloads.
Instead, records missing an attribute required by a placeholder
will be redirected to error output if no default is provided.

### Protocol

When `--outputType=file`, `--output` may be prefixed by a protocol specifier 
to determine the
target data store. Without a protocol prefix, the output path is assumed
to be a relative or absolute path on the filesystem. To write to Google
Cloud Storage, use a `gs://` path like:

    --output=gs://mybucket/somdir/myfileprefix

### Attribute placeholders

We support `FileIO`'s "Dynamic destinations" feature (`FileIO.writeDynamic`) where
it's possible to route individual messages to different output locations based
on properties of the message.
In our case, we allow routing messages based on the `PubsubMessage` attribute map.
Routing is accomplished by adding placeholders of form `${attribute_name:-default_value}`
to the path.

For example, to route based on a `document_type` attribute, your path might look like:

    --output=gs://mybucket/mydocs/${document_type:-UNSPECIFIED}/myfileprefix

Messages with `document_type` of "main" would be grouped together and end up in
the following directory:

    gs://mybucket/mydocs/main/

Messages with `document_type` set to `null` or missing that attribute completely
would be grouped together and end up in directory:

    gs://mybucket/mydocs/UNSPECIFIED/

Note that placeholders _must_ specify a default value so that a poorly formatted
message doesn't cause a pipeline exception. A placeholder without a default will
result in an `IllegalArgumentException` on pipeline startup.

File-based outputs support the additional _derived_ attributes
`"submission_date"` and `"submission_hour"` which will be parsed from the value
of the `submission_timestamp` attribute if it exists.
These can be useful for making sure your output specification buckets messages
into hourly directories.

The templating and default syntax used here is based on the
[Apache commons-text `StringSubstitutor`](https://commons.apache.org/proper/commons-text/javadocs/api-release/org/apache/commons/text/StringSubstitutor.html),
which in turn bases its syntax on common practice in bash and other Unix/Linux shells.
Beware the need for proper escaping on the command line (use `\$` in place of `$`),
as your shell may try to substitute in values
for your placeholders before they're passed to `Sink`.

[Google's PubsubMessage format](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)
allows arbitrary strings for attribute names and values. We place the following restrictions
on attribute names and default values used in placeholders:

- attribute names may not contain the string `:-`
- attribute names may not contain curly braces (`{` or `}`)
- default values may not contain curly braces (`{` or `}`)

### File prefix

Individual files are named using the default format discussed in
the "File naming" section of Beam's
[`FileIO` Javadoc](https://beam.apache.org/documentation/sdks/javadoc/2.6.0/org/apache/beam/sdk/io/FileIO.html):

    $prefix-$start-$end-$pane-$shard-of-$numShards$suffix$compressionSuffix

In our case, `$prefix` is determined from the last `/`-delimited piece of the `--output`
path. If you specify a path ending in `/`, you'll end up with an empty prefix
and your file names will begin with `-`. This is probably not what you want, 
so it's recommended to end your output path with a non-empty file prefix.

For example, given:

    --output=/tmp/output/out

An output file might be:

    /tmp/output/out--290308-12-21T20:00:00.000Z--290308-12-21T20:10:00.000Z-00000-of-00001.ndjson

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

# check the help page to see types of options
./bin/mvn compile exec:java -Dexec.args=--help

# check the SinkOptions help page for options specific to Sink
./bin/mvn compile exec:java -Dexec.args=--help=SinkOptions
```

### On Dataflow

```bash
# Pick a bucket to store files in
BUCKET="gs://$(gcloud config get-value project)"

# create a test input file
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' | gsutil cp - $BUCKET/input.json

# Set credentials; beam is not able to use gcloud credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/creds.json"

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

Dataflow templates make a distinction between
[runtime parameters that implement the `ValueProvider` interface](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates#runtime-parameters-and-the-valueprovider-interface)
and compile-time parameters which do not.
All option can be specified at template compile time by passing command line flags,
but runtime parameters can also be overridden when
[executing the template](https://cloud.google.com/dataflow/docs/guides/templates/executing-templates#using-gcloud)
via the `--parameters` flag.
In the output of `--help=SinkOptions`, runtime parameters are those 
with type `ValueProvider`.

```bash
# Pick a bucket to store files in
BUCKET="gs://$(gcloud config get-value project)"

# Set credentials; beam is not able to use gcloud credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/creds.json"

# create a template
./bin/mvn compile exec:java -Dexec.args="\
    --runner=Dataflow \
    --project=$(gcloud config get-value project) \
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

### In streaming mode

If `--inputType=pubsub`, Beam will execute in streaming mode, requiring some
extra configuration for file-based outputs. You will need to specify sharding like:

```
    --outputNumShards=10
    --errorOutputNumShards=10
```

As discussed in the
[Beam documentation for `FileIO.Write#withNumShards`](https://beam.apache.org/releases/javadoc/2.8.0/org/apache/beam/sdk/io/FileIO.Write.html#withNumShards-int-),
batch mode is most efficient when the runner is left to determine sharding,
so `numShards` options should normally be left to their default of `0`, but
streaming mode can't perform the same optimizations thus an exception will be thrown
during pipeline construction if sharding is not specified.
As codified in [apache/beam/pull/1952](https://github.com/apache/beam/pull/1952),
the Dataflow runner suggests a reasonable starting point `numShards` is `2 * maxWorkers`
or 10 if `--maxWorkers` is unspecified.

# Decoder Job

A job for normalizing ingestion messages.

## Transforms

These transforms are currently executed against each message in order.

### Parse URI

Attempt to extract attributes from `uri`, on failure send messages to the
configured error output.

### Decompress

Attempt to decompress payload with gzip, on failure pass the message through
unmodified.

### GeoIP Lookup

1. Extract `ip` from the `x_forwarded_for` attribute
   * when the `x_pipeline_proxy` attribute is not present, use the
     second-to-last value (since the last value is a forwarding rule IP
     added by Google load balancer)
   * when the `x_pipeline_proxy` attribute is present, use the third-to-last
     value (since the tee introduces an additional proxy IP)
   * fall back to the `remote_addr` attribute, then to an empty string
1. Execute the following steps until one fails and ignore the exception
    1. Parse `ip` using `InetAddress.getByName`
    1. Lookup `ip` in the configured `GeoIP2City.mmdb`
    1. Extract `country.iso_code` as `geo_country`
    1. Extract `city.name` as `geo_city`
    1. Extract `subdivisions[0].iso_code` as `geo_subdivision1`
    1. Extract `subdivisions[1].iso_code` as `geo_subdivision2`
1. Remove the `x_forwarded_for` and `remote_addr` attributes
1. Remove any `null` values added to attributes

### Parse User Agent

Attempt to extract browser, browser version, and os from the `user_agent`
attribute, drop any nulls, and remove `user_agent` from attributes.

## Executing Decoder Jobs

Decoder jobs are executed the same way as [executing sink jobs](#executing-jobs)
but with a few extra flags:

 * `-Dexec.mainClass=com.mozilla.telemetry.Decoder`
 * `--geoCityDatabase=/path/to/GeoIP2-City.mmdb`

Example:

```bash
# create a test input file
mkdir -p tmp/
echo '{"payload":"dGVzdA==","attributeMap":{"remote_addr":"63.245.208.195"}}' > tmp/input.json

# Download `GeoLite2-City.mmdb` and `schemas.tar.gz`
./bin/download-geolite2
./bin/download-schemas

# do geo lookup on messages to stdout
./bin/mvn compile exec:java -Dexec.mainClass=com.mozilla.telemetry.Decoder -Dexec.args="\
    --geoCityDatabase=GeoLite2-City.mmdb \
    --schemasLocation=schemas.tar.gz \
    --inputType=file \
    --input=tmp/input.json \
    --outputType=stdout \
    --errorOutputType=stderr \
    --seenMessagesSource=none \
"

# check the DecoderOptions help page for options specific to Decoder
./bin/mvn compile exec:java -Dexec.args=--help=DecoderOptions
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
