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
    --geoCityDatabase=gs://moz-fx-data-prod-geoip/GeoIP2-City/20241105/GeoIP2-City.mmdb \
    --geoCityFilter=gs://moz-fx-data-prod-dataflow-templates/cities15000.txt \
    --geoIspDatabase=gs://moz-fx-data-prod-geoip/GeoIP2-ISP/20241101/GeoIP2-ISP.mmdb \
    --schemasLocation=${BUCKET}/schemas/schemas.tar.gz \
    --allowedNamespaces=org-mozilla-ios-firefox,org-mozilla-ios-firefoxbeta \
    --allowedDocTypes=events \
    --inputType=file \
    --input=$path \
    --bqReadMethod=storageapi \
    --outputType=bigquery \
    --bqWriteMethod=file_loads \
    --output=${PROJECT}:test.output_v1 \
    --errorOutputType=stderr \
    --gcsUploadBufferSizeBytes=16777216 \
    --tempLocation=${BUCKET}/temp/bq-loads \
    --eventsAllowList=${BUCKET}/eventsAllowlist.csv \
    --apiKey=${BUCKET}/apiKey \
    --maxEventBatchSize=50 \
    --region=us-central1 \
"
