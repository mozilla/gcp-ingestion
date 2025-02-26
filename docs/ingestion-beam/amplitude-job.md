# Amplitude Publisher Job

The Amplitude publisher job sends a specific set of events to the Amplitude batch API. This job is currently used to run a proof-of-concept evaluation of Amplitude on a subset of events. For more context see: https://mozilla-hub.atlassian.net/browse/DENG-7616

The code is defined in the [`com.mozilla.telemetry.AmplitudePublisher`](https://github.com/mozilla/gcp-ingestion/blob/main/ingestion-beam/src/main/java/com/mozilla/telemetry/AmplitudePublisher.java) class.

This job reads from per doctype Pub/Sub topics of specific applications.

## Beam Pipeline Transforms

The goal is to have event data in Amplitude that is as close as possible to the data used for calculating KPIs and is used for other creating other datasets in BigQuery. Therefore, the transformation steps closely resemble [those of the Decoder job](https://mozilla.github.io/gcp-ingestion/ingestion-beam/decoder-job/).

### Pub/Sub republished topic

The input to this job is the subset of decoded messages from various namespaces that have pings with event data.

### Parse URI

Attempt to extract attributes from `uri`, on failure send messages to the
configured error output.

### Decompress

Attempt to decompress payload with gzip, on failure pass the message through
unmodified.

### Filter by Doc Type and Namespaces

Depending on what Pub/Sub topic the job is configured to read from, this will allow to filter for specific namespaces and doc types that should be processed further.

### [optional] Sampling

Optionally, a sampling can be configured for the job. This is to reduce the number of events that get sent to the Amplitude API.

### Parse Payload

Parse the message body as a `UTF-8` encoded JSON payload and drop some specific fields.

### Parse User Agent

Attempt to extract browser, browser version, and os from the `user_agent`
attribute, drop any nulls, and remove `user_agent` from attributes.

### Write Metadata Into the Payload

Add a nested `metadata` field and several `normalized_*` attributes into the
payload body.

### Parse Amplitude Events

This step reads an external CSV configuration file that contains a set of events that should be sent do Amplitude. The format of this file is as follows:

```csv
<namespace>,<doc type>,<event category>,<event name or '*'>
```

The `events` are getting parsed from the message payload and transformed into individual events that can be sent to the Amplitude API. Events that don't match the allow list are being filtered out.

### Batching of Events

Since the Amplitude API has [limitations](https://amplitude.com/docs/apis/analytics/batch-event-upload#considerations) around how many events can be sent in a single request and how many requests can be sent withing a second, this step creates batches of events.

### Send request

This step sends batched events to the Amplitude API.
// todo: retry mechanism on failures which can happen when API limits are exceeded

### Working with the Beam Job

Options specific to this job are found in https://github.com/mozilla/gcp-ingestion/blob/main/ingestion-beam/src/main/java/com/mozilla/telemetry/amplitude/AmplitudePublisherOptions.java

### Test Deployment

This job can be deployed in a sandbox project for testing.

There are a few required components to get a job running:

- Upload a `.ndjson` file with example payload data to a GCS bucket
- A event allowed list stored in GCS
- Optionally, if reporting is enabled, a file with the Amplitude API key uploaded to GCS
- The Beam pipeline running on Dataflow, reading from the input, and sending data to Amplitude

Example script to start the Dataflow job from the ingestion-beam directory:

```
#!/bin/bash

set -ux

PROJECT="amplitude-dev"
JOB_NAME="amplitude"
path="$BUCKET/data/*.ndjson"

mvn -X compile exec:java -Dexec.mainClass=com.mozilla.telemetry.AmplitudePublisher -Dexec.args="\
    --runner=Dataflow \
    --jobName=$JOB_NAME \
    --project=$PROJECT  \
    --inputType=file \
    --input=$path \
    --bqReadMethod=storageapi \
    --outputType=bigquery \
    --bqWriteMethod=file_loads \
    --errorOutputType=stderr \
    --tempLocation=amplitude-data-dev/temp/bq-loads \
    --eventsAllowList=amplitude-data-dev/eventsAllowlist.csv \
    --apiKeys=amplitude-data-dev/apiKeys.csv \
    --region=us-central1 \
"
```
