#!/bin/bash

set -exo pipefail

PROJECT=""
JOB_NAME="amplitude-test"
BUCKET="gs://$PROJECT"

path="$BUCKET/data/*.ndjson"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# this script assumes it's being run from the ingestion-beam directory
# of the gcp-ingestion repo.

$SCRIPT_DIR/mvn -X compile exec:java -Dexec.mainClass=com.mozilla.telemetry.AmplitudePublisher -Dexec.args="\
    --runner=Dataflow \
    --jobName=$JOB_NAME \
    --project=$PROJECT  \
    --allowedNamespaces=org-mozilla-ios-firefox,org-mozilla-ios-firefoxbeta \
    --allowedDocTypes=events,metrics \
    --inputType=file \
    --input=$path \
    --bqReadMethod=storageapi \
    --outputType=bigquery \
    --bqWriteMethod=file_loads \
    --errorOutputType=stderr \
    --tempLocation=${BUCKET}/temp/bq-loads \
    --eventsAllowList=${BUCKET}/eventsAllowlist.csv \
    --apiKeys=${BUCKET}/apiKeys.csv \
    --maxEventBatchSize=50 \
    --region=us-central1 \
"
