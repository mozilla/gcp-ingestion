# Decoder Service Specification

This document specifies the behavior of the service that decodes messages
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
1. Write message to PubSub decoded topic based on `namespace` and `docType`
1. Mark `docId` as seen in deduplication storage

### Implementation

The above steps will be executed as a single Apache Beam job that can accept
either a streaming input from PubSub or a batch input from Cloud Storage.
Message deduplication will be done by checking for the presence of ids as keys
in Cloud Memory Store (managed Redis), and adding ids to Memory Store after
successful delivery to PubSub.

### Decoding Errors

All messages that are rejected at any step of the Data Flow above will be
forwarded to a PubSub error topic, for backfill and monitoring purposes.
This includes duplicate messages based on `docId`.

#### Error message schema

The message that failed decoding, with two additional attributes:

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

### Decoded message metadata schema

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

Messages should only be acknowledged in the PubSub raw topic subscription after
delivery to either a decoded topic or the error topic.

If this is not possible then any time a message is not successfully delivered
to PubSub it should by treated as lost data and the appropriate time window
will be backfilled from Cloud Storage in batch mode, and appropriate steps will
be taken downstream to handle the backfill.

Deployments should always terminate functional pipelines using the `drain`
method, to ensure ack'd messages are fully delivered.

### Deduplication

Each `docId` will be allowed through "at least once", and only be
rejected as a duplicate if we have completed delivery of a message with the
same `docId`. Duplicates will be considered errors and sent to the error topic.
"Exactly once" semantics can be applied to derived data sets using SQL in
BigQuery, and GroupByKey in Beam and Spark.

## Testing

Always have 100% branch and statement test coverage for code that is in
production. If a statement has variable behavior without branching, test all
such variations.

### CI Testing

CI will test all of the behavior described in [Data Flow](#data-flow)
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

Before being deployed to production each release should have these tests run:

 * CI Tests using actual Google Cloud services
 * Load test
   * With and without a simulated PubSub outage.
   * Use 1.5x peak production traffic volume over the last month. As of
     2018-08-01 peak production traffic volume is about 11K req/s.
   * Handle PubSub returning 500 for at least 1.5x the longest PubSub outage in
     the last year. As of 2018-08-01 the longest outage was about 4.5 hours.
   * Non-outage load tests for 50% invalid messages and 5% invalid messages, with
     time limits to ensure that we aren't incurring excessive processing delays.
