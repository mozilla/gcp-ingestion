# Validation Service Specification

This document specifies the behavior of the service that validates messages
in the Structured Ingestion pipeline.

## Data Flow

1. Consume messages from Google Cloud PubSub raw topic
1. Decode the body from base64, optionally gzip, and JSON
1. Validate the schema of the body
1. Perform GeoIP lookup and drop `x_forwarded_for` and `remote_addr`
1. Extract user agent information and drop `user_agent`
1. Add metadata fields to message
1. Deduplicate message by `docId`
   * Generate `docId` for submission types that don't have one
1. Write message to PubSub validated topic based on namespace and `docType`
1. Mark `docId` as seen in deduplication storage

### Implementation

Execute the above steps as a single Dataflow job that can accept either a
streaming input from PubSub or a batch input from Cloud Storage. Deduplicate
messages by checking for the presence of `docId` as a key in Cloud Memory Store
(managed Redis), and adding `docId` to Memory Store after successful delivery
to PubSub.

### Validation Errors

Forward all messages that are rejected at any step of the Data Flow above to a
PubSub error topic, for backfill and monitoring purposes. This includes
duplicate messages based on `docId`.

#### Error message schema

[Edge Server PubSub Message Schema](edge.md#edge-server-pubsub-message-schema)
with two additional fields:

```
...
required group attributes {
  ...
  required string error_type    // example: "schema"
  required string error_message // example: "message did not match json schema for <namespace>/<docVersion>/<docType>"
}
```

### Raw message schema

See [Edge Server PubSub Message Schema](edge.md#edge-server-pubsub-message-schema).

### Validated message metadata schema

Schema of the metadata object that is added to the message:

```
required group metadata {
  required string document_version           // from uri for non-Telemetry, from message for Telemetry
  required string document_id                // from uri
  required string geo_country                // from geoip lookup
  required string geo_subdivision1           // from geoip lookup
  required string geo_subdivision2           // from geoip lookup
  required string geo_city                   // from geoip lookup
  required string submission_timestamp       // from edge metadata
  optional string date                       // header from client
  optional string dnt                        // header from client
  optional string x_pingsender_version       // header from client
  optional string user_agent_browser         // from user_agent
  optional string user_agent_browser_version // from user_agent
  optional string user_agent_os              // from user_agent
  optional string user_agent_os_version      // from user_agent
}
```

## Other Considerations

### Message Acks

Only acknowledge messages in the PubSub raw topic subscription after delivery
to either a validated topic or the error topic.

If that is not possible then any time a message is not successfully delivered
to PubSub treat it as lost data and backfill the appropriate time window from
Cloud Storage in batch mode, and take appropriate steps downstream to handle
the backfill.

Always use the `drain` method to terminate functional pipelines, to ensure
ack'd messages are fully delivered whenever possible.

### Deduplication

Allow each `docId` through "at least once", and only reject it as a duplicate
if we have completed delivery of a message with the same `docId`. Treat
duplicates as errors and send them to the error topic.

"Exactly once" semantics can be achieved in later stages of the pipeline using
SQL in BigQuery, and GroupByKey in Dataflow and Spark.

## Testing

Test the full expected behavior of the service.

### CI Testing

Run CI tests for all of the behavior described in [Data Flow](#data-flow)
including, but not limited to:

 * A valid message for every combination of
   * `namespace`
   * `docType`
   * `docVersion`
   * Gzipped body
   * Present, absent, and gibberish optional PubSub message attribute fields
 * An invalid message for every combination, for each of the following reasons
   * Invalid body field values
   * Invalid body field types
   * Invalid body JSON
   * Invalid gzip data
   * Invalid `uri`
   * Invalid `docId`
   * Duplicate `docId`
   * Unknown `namespace`
   * Unknown `docType`
   * Unknown `docVersion`
 * In batch mode and streaming mode

### Pre-deployment Testing

Run the following tests for each release before deploying to production:

 * CI Tests using actual Google Cloud services
 * Load test
   * Simulate a PubSub outage.
   * Use 1.5x peak production traffic volume over the last month. As of
     2018-08-01 peak production traffic volume is about 11K req/s.
   * Use 50% invalid messages and 5% invalid messages
   * Impose a time limit to ensure that we aren't incurring excessive
     processing delays.
