# Decoder Service Specification

This document specifies the behavior of the service that decodes messages
in the Structured Ingestion pipeline.

## Deployment topology

Decoder runs as one Dataflow job per (pipeline family, input topic). Each pipeline
family has its own `<family>-raw`, `<family>-decoded`, and `<family>-error` topics:
`telemetry` (legacy Firefox telemetry), `structured` (Glean and other structured pings),
and `stub_installer`. The `structured` family also has a `structured-logging` input
topic served by a second decoder job with `--logIngestionEnabled=true`. Topics and jobs
are defined in
[cloudops-infra](https://github.com/mozilla-services/cloudops-infra/tree/master/projects/beam).

## Ingestion sources

Each decoder instance accepts one input wire format, selected at deploy time via the
`--logIngestionEnabled` and `--directPubsubEnabled` flags (defaulting to Edge if neither
is set):

- Edge (HTTP) - default, messages from `ingestion-edge`. See
  [Edge Service PubSub Message Schema](edge_service_specification.md#pubsub-message-schema).
- Cloud Logging (LogEntry) - enabled with `--logIngestionEnabled=true`.
  Glean publishers log to stdout, Cloud Logging Sink ships
  LogEntry-wrapped messages to `structured-logging`.
- Direct Pub/Sub - enabled with `--directPubsubEnabled=true`. Server-side
  publishers write Glean ping JSON (optionally gzipped) as message body, with
  ping metadata fields as message attributes. Required attributes:
  `document_namespace`, `document_type`, `document_version`, `document_id`;
  messages missing any of these are routed to the error topic. Optional
  attributes: `user_agent`, `x_forwarded_for`. `submission_timestamp` is
  stamped by the decoder from Pub/Sub's publishTime if not already set.

## Data Flow

1. Consume messages from the job's input Pub/Sub topic (see [Deployment
   topology](#deployment-topology))
1. Deduplicate message by `uri` (which generally contains `docId`)
   - Disabled for stub-installer, which does not include a UUID in the URI
1. Perform GeoIP lookup and drop `x_forwarded_for` and `remote_addr` and
   optionally `geo_city` based on population
1. Parse the `uri` attribute to determine document type, etc.
1. Decode the body from base64, optionally decompress, and parse as JSON
1. Scrub the message, checking the content against a list of known signatures
   that should cause the message to be dropped as toxic, sent to error output,
   or to have specific fields redacted
1. Validate the schema of the body
1. Extract user agent information and drop `user_agent`
1. Sanitize metadata based on per-document type configuration
1. Add metadata fields to message
1. Write message to the job's PubSub decoded topic (`<family>-decoded`)

### Implementation

The above steps are executed as a single Apache Beam pipeline that can accept either
streaming input from Pub/Sub or batch input from Cloud Storage. The same pipeline runs
as multiple Dataflow jobs in production, one per (pipeline family, input topic),
parameterized by topic arguments and the `--logIngestionEnabled` /
`--directPubsubEnabled` flags.

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

See [Edge Service PubSub Message Schema](edge_service_specification.md#pubsub-message-schema).

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
  optional string x_foxsec_ip_reputation     // header from iprepd
  optional string x_lb_tags                  // header from load balancer
  optional string x_source_tags              // header from client
  optional string x_telemetry_agent          // header from client
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

Each `uri` will be allowed through "at least once", and only be
rejected as a duplicate if we have completed delivery of a message with the
same `uri`. We assume that each `uri` contains a UUID that uniquely identifies
the document.
"Exactly once" semantics can be applied to derived data sets using SQL in
BigQuery, and GroupByKey in Beam and Spark.

Note that deduplication is only provided with a "best effort" quality of service
using a 10 minute window.
