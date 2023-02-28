#!/bin/bash

set -ux

PROJECT="contextual-services-dev"
JOB_NAME="contextual-services-reporter-$(whoami)"

mvn compile exec:java -Dexec.mainClass=com.mozilla.telemetry.ContextualServicesReporter -Dexec.args="\
   --runner=Dataflow \
   --jobName=$JOB_NAME \
   --project=$PROJECT  \
   --inputType=pubsub \
   --input='projects/contextual-services-dev/subscriptions/ctxsvc-input' \
   --outputTableRowFormat=payload \
   --errorBqWriteMethod=streaming \
   --errorOutputType=bigquery \
   --errorOutput=$PROJECT:contextual_services.reporting_errors \
   --region=us-central1 \
   --usePublicIps=true \
   --gcsUploadBufferSizeBytes=16777216 \
   --urlAllowList=gs://contextual-services-data-dev/urlAllowlist.csv \
   --allowedDocTypes=topsites-impression,topsites-click, \
   --allowedNamespaces=org-mozilla-fenix,org-mozilla-firefox-beta \
   --aggregationWindowDuration=10m \
   --clickSpikeWindowDuration=3m \
   --clickSpikeThreshold=10 \
   --reportingEnabled=false \
   --logReportingUrls=true \
   --maxNumWorkers=2 \
   --numWorkers=1 \
   --autoscalingAlgorithm=THROUGHPUT_BASED \
"