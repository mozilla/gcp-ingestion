[![CircleCI](https://circleci.com/gh/mozilla/gcp-ingestion.svg?style=svg&circle-token=d98a470269580907d5c6d74d0e67612834a21be7)](https://circleci.com/gh/mozilla/gcp-ingestion)

# Sink Service

A Dataflow job for delivering messages between Google Cloud services.

## Supported Services

Supported inputs:

 * Google Cloud PubSub
 * Google Cloud Storage

Supported outputs:

 * Google Cloud PubSub
 * Google Cloud Storage
 * stdout (Java direct runner only)

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

### Locally

```bash
# create a test input file
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' > /tmp/input.json

# consume messages from the test file, decode and re-encode them, and write to a directory
sbt 'run
    --inputFileFormat=json
    --inputType=file
    --input=/tmp/input.json
    --outputFileFormat=json
    --outputType=file
    --output=/tmp/output
'

# check that the message was delivered
cat /tmp/output/*

# write message payload straight to stdout
sbt 'run
    --inputFileFormat=json
    --inputType=file
    --input=/tmp/input.json
    --outputFileFormat=text
    --outputType=stdout
'

# check the help page to see all options
sbt 'run --help=Options'
```

### On Dataflow

```bash
# Pick a bucket to store files in
BUCKET="gs://$(gcloud config get-value project)"

# create a test input file
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' | gsutil cp - $BUCKET/input.json

# consume messages from the test file, decode and re-encode them, and write to a bucket
sbt "run
    --runner=Dataflow
    --inputFileFormat=json
    --inputType=file
    --input=$BUCKET/input.json
    --outputFileFormat=json
    --outputType=file
    --output=$BUCKET/output
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
sbt "run
    --runner=Dataflow
    --inputFileFormat=json
    --inputType=file
    --outputFileFormat=json
    --outputType=file
    --templateLocation=$BUCKET/sink/templates/JsonFileToJsonFile
    --stagingLocation=$BUCKET/sink/staging
"

# create a test input file
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' | gsutil cp - $BUCKET/input.json

# run the dataflow template with gcloud
JOBNAME=FileToFile1
gcloud dataflow jobs run $JOBNAME --gcs-location=$BUCKET/sink/templates/JsonFileToJsonFile --parameters "input=$BUCKET/input.json,output=$BUCKET/output/"

# get the job id
JOB_ID="$(gcloud dataflow jobs list --filter name=fileToStdout1 | tail -1 | cut -d' ' -f1)"

# wait for the job to finish
gcloud dataflow jobs show "$JOB_ID"

# check that the message was delivered
gsutil cat $BUCKET/output/*
```

## Testing

Run tests locally with [CircleCI Local CLI](https://circleci.com/docs/2.0/local-cli/#installing-the-circleci-local-cli-on-macos-and-linux-distros)

```bash
(cd .. && circleci build --job sink)
```

# License

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
