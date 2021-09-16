# Contextual Services Reporter Job

The contextual services reporter forwards contextual-services click and impression events
to an external partner, with a particular eye towards minimizing the scope of contextual metadata
that is shared. For more context, see the
[Data and Firefox Suggest blog post](https://blog.mozilla.org/data/2021/09/15/data-and-firefox-suggest/).

The code is defined in the [`com.mozilla.telemetry.ContextualServicesReporter`](https://github.com/mozilla/gcp-ingestion/blob/main/ingestion-beam/src/main/java/com/mozilla/telemetry/ContextualServicesReporter.java) class.

The input of this job is all `contextual-services` namespace messages, which includes `topsites-impression`, `topsites-click`, `quicksuggest-impression`, and `quicksuggest-click`.

## Beam Pipeline Transforms

The following diagram shows the steps in the Beam pipeline. This is up to date as of 2021-08-23.

![diagrams/workflow.mmd](../../diagrams/ctxtsvc-sender.svg)
**Figure**: _An overview of the execution graph for the `ContextualServicesReporter`._

### Pub/Sub republished topic

The input to this job is the subset of decoded messages in the `contextual-services` namespace.

### `FilterByDoctype`

This step filters out document types based on the `document_type` attribute using the `--allowedDocTypes` pipeline option. For example, this can be used to allow the job to process only sponsored tiles or only Suggest events, or only clicks or only impressions.

### `VerifyMetadata`

Message contents are validated in this step using a few simple heuristics such as user agent and user agent version. Messages that fail this check are rejected and sent to the error table.

### `DecompressPayload`

This step attempts to decompress a gzip-compressed payload. This transform is shared with other Beam pipelines such as the decoder.

### `ParseReportingUrl`

This is where the URLs used for reporting events are built. The `reporting_url` value from the message payload is used as the base URL, then additional query parameters are added based on the message metadata such as the client’s country, region, and OS. The updated `reporting_url` value is put in the message payload and attributes.

### `LabelClickSpikes`

This step counts the number of click events per client (using the `context_id` attribute) in a time interval. This transform uses Beam’s state and timers; a state and a timer is maintained for every client. The state is a list of recent timestamps of clicks from the current client and the timer will clear the state if there are no recent clicks from the client. If the number of elements in the list exceeds the set threshold, any additional clicks will be marked with a `click-status`. Because a state needs to be maintained per client, the memory required for this step increases with the number of unique clients.

### `AggregateImpressions`

This step groups impressions together in a timed window based on the reporting URLs. The purpose of this step is to reduce the number of HTTP requests made to external endpoints. One URL is output for each unique reporting URL in the timed window by counting the number of occurrences of each URL. The output URL is the original reporting URL with the additional query parameters `impressions`, `begin-timestamp`, and `end-timestamp`.

A Beam `IntervalWindow` is used to group by impressions together based on the processing time - the time at which the message enters this transform. The output is generated at the end of the window which means that there is an increased delay between when the impression enters the pipeline and when the corresponding reporting URL is requested. This transform needs to keep track of a window for every unique reporting URL it receives which means memory required increases with the number of unique URLs.

### `SendRequest`

This step sends a HTTP GET request to the URL specified by the `reporting_url` attribute in the message. To keep track of requests sent by the job for debugging purposes, successful requests will throw a `RequestContentException` which will add them to the BigQuery error table (see below).

### BigQuery Error Table

The job is configured at certain steps to catch runtime exceptions and write the message contents to BigQuery. The configured table is `moz-fx-data-shared-prod.payload_bytes_error.contextual_services`. This can be used for debugging or to backfill messages that initially failed to process.

### Working with the Beam Job

Options specific to this job are found in https://github.com/mozilla/gcp-ingestion/blob/main/ingestion-beam/src/main/java/com/mozilla/telemetry/contextualservices/ContextualServicesReporterOptions.java

### Test Deployment

This job can be deployed in a sandbox project for testing. The `contextual-services-dev` project is currently used for this purpose.

There are a few required components to get a job running:

- A PubSub subscription on the republished `contextual-services` topic
- A BigQuery table with the `payload_bytes_error` schema used for error output
- A URL allowed list stored in GCS
- The Beam pipeline running on Dataflow, reading from the PubSub subscription, and writing to the BigQuery table

Example script to start the Dataflow job from the ingestion-beam directory:

```
#!/bin/bash

set -ux

PROJECT="contextual-services-dev"
JOB_NAME="contextual-services"

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
   --reportingEnabled=false \
   --aggregationWindowDuration=10m \
   --clickSpikeWindowDuration=3m \
   --clickSpikeThreshold=10 \
   --logReportingUrls=true \
   --maxNumWorkers=2 \
   --numWorkers=1 \
   --autoscalingAlgorithm=THROUGHPUT_BASED \
"
```
