[![CircleCI](https://circleci.com/gh/mozilla/gcp-ingestion/tree/master.svg?style=svg)](https://circleci.com/gh/mozilla/gcp-ingestion/tree/master)

# Landfill Service

A Dataflow job for delivering messages between Google Cloud PubSub and Google
Cloud Storage.

## Building

```bash
sbt compile
```

## Running

To run the job locally:

```bash
# create a test input file
echo '{"payload":"test","attributeMap":{"host":"test"}}' > /tmp/input.json

# consume messages from the test file, decode and re-encode them, and write to a directory
sbt 'run --inputType=file --input=/tmp/input.json --outputType=file --output=/tmp/output'

# check that the message was delivered
cat /tmp/output/*

# or just write messages straight to stdout
sbt 'run --inputType=file --input=/tmp/input.json --outputType=stdout --output='

# check the help page to see all options
sbt 'run --help=Options'
```

To run the job on Dataflow

```bash
# Pick a bucket to store files in
BUCKET="gs://$(gcloud config get-value project)"

# create a test input file
echo '{"payload":"test","attributeMap":{"host":"test"}}' | gsutil cp - $BUCKET/input.json

# consume messages from the test file, decode and re-encode them, and write to a bucket
sbt "run --runner=Dataflow --inputType=file --input=$BUCKET/input.json --outputType=file --output=$BUCKET/output/"

# wait for the job to finish
gcloud dataflow jobs list

# check that the message was delivered
gsutil cat $BUCKET/output/*
```

To run the job on Dataflow with templates

```bash
# Pick a bucket to store files in
BUCKET="gs://$(gcloud config get-value project)"

# create a template, with inputType and outputType of "file"
sbt "run --inputType=file --outputType=file --runner=Dataflow --templateLocation=$BUCKET/landfill/templates/FileToFile --stagingLocation=$BUCKET/landfill/staging"

# create a test input file
echo '{"payload":"test","attributeMap":{"host":"test"}}' | gsutil cp - $BUCKET/input.json

# run the dataflow template with gcloud
JOBNAME=FileToFile1
gcloud dataflow jobs run $JOBNAME --gcs-location=$BUCKET/landfill/templates/FileToFile --parameters "input=$BUCKET/input.json,output=$BUCKET/output/"

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
(cd .. && circleci build --job landfill)
```

# License

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
