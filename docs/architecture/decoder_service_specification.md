# Decoder Service Specification

This document specifies the behavior of the service that decodes messages
in the Structured Ingestion pipeline.

## Data Flow

1. Consume messages from Google Cloud PubSub raw topic
1. Decode the body from base64, optionally gzip, and JSON
1. Validate the schema of the body
1. Perform GeoIP lookup and drop `x_forwarded_for` and `remote_addr` and
   optionally `geo_city` based on population
1. Extract user agent information and drop `user_agent`
1. Add metadata fields to message
1. Deduplicate message by `docId`
   * Generate `docId` for submission types that don't have one
1. Write message to PubSub decoded topic based on `namespace` and `docType`

### Implementation

The above steps will be executed as a single Apache Beam job that can accept
either a streaming input from PubSub or a batch input from Cloud Storage.
Message deduplication will be done by checking for the presence of ids as keys
in Cloud Memory Store (managed Redis).

### Decoding Errors

All messages that are rejected at any step of the Data Flow above will be
forwarded to a PubSub error topic for backfill and monitoring purposes.
If we determine that a message has already been successfully processed
based on `docId`, we drop the duplicated body and publish just the metadata
to the error topic.

#### Error message schema

The message that failed decoding, with several additional attributes:

```
...
required group attributes {
  ...
  required string error_type    // example: "schema"
  required string error_message // example: "message did not match json schema for <namespace>/<docVersion>/<docType>"
  required string exception_class // example: "java.lang.RuntimeException"
  required string stack_trace
  optional string stack_trace_cause_1
  optional string stack_trace_cause_2
  optional string stack_trace_cause_3
  optional string stack_trace_cause_4
  optional string stack_trace_cause_5
}
```

### Raw message schema

See [Edge Server PubSub Message Schema](edge_server_specification.md#edge-server-pubsub-message-schema).

### Decoded message metadata schema

Decoded messages published to Pub/Sub will contain the following attributes:

```
required group attributes {
  ...
  required string document_version           // from uri for non-Telemetry, from message for Telemetry
  required string document_id                // from uri
  required string document_namespace         // from uri
  required string document_type              // from uri
  optional string app_name                   // from uri for Telemetry
  optional string app_version                // from uri for Telemetry
  optional string app_update_channel         // from uri for Telemetry
  optional string app_build_id               // from uri for Telemetry
  optional string geo_country                // from geoip lookup
  optional string geo_subdivision1           // from geoip lookup
  optional string geo_subdivision2           // from geoip lookup
  optional string geo_city                   // from geoip lookup
  required string submission_timestamp       // from edge metadata
  optional string date                       // header from client
  optional string dnt                        // header from client
  optional string x_pingsender_version       // header from client
  optional string x_debug_id                 // header from client
  optional string user_agent_browser         // from user_agent
  optional string user_agent_browser_version // from user_agent
  optional string user_agent_os              // from user_agent
  optional string user_agent_os_version      // from user_agent
  optional string normalized_app_name        // based on parsed json payload
  optional string normalized_channel         // based on parsed json payload or URI
  required string normalized_country_code    // from geoip lookup
  optional string normalized_os              // based on parsed json payload
  optional string normalized_os_version      // based on parsed json payload
  optional string sample_id                  // based on parsed json payload
}
```

Many of these fields are also injected into the JSON payload either at the top
level or nested inside a `metadata` object. The schema for injected metadata
is maintained under the [`metadata` namespace in `mozilla-pipeline-schemas`](https://github.com/mozilla-services/mozilla-pipeline-schemas/tree/dev/schemas/metadata).

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
