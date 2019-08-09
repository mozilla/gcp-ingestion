# BigQuery Sink Service Specification

This document specifies the behavior of the service that delivers decoded
messages into BigQuery.



## Data Flow

Consume messages from a PubSub topic or Cloud Storage location and insert them
into BigQuery. Send errors to another PubSub topic or Cloud Storage location.

### Implementation

Execute this as an Apache Beam job.

### Configuration

Require configuration for:

 * The input PubSub topic or Cloud Storage location
 * The route map from PubSub message attributes to output BigQuery table
 * The error output PubSub topic or Cloud Storage location

Accept optional configuration for:

 * The fallback output PubSub topic for messages with no route
 * The output mode for BigQuery, default to `mixed`
 * List of document types to opt-in for streaming when running in `mixed`
   output mode
 * The triggering frequency for writing to BigQuery, when output mode is
   `file_loads`

### Coerce Types

Reprocess the JSON payload in each message to match the schema of the
destination table found in BigQuery as codified by the
[`jsonschema-transpiler`](https://github.com/mozilla/jsonschema-transpiler).

Support the following logical transformations:

  * Transform key names to replace `-` and `.` with `_`
  * Transform key names beginning with a number by prefixing with `_`
  * Transform map types to arrays of key/value maps when the destination
    field is a repeated `STRUCT<key, value>`
  * Transform complex types to JSON strings when the destination field
    in BigQuery expects a string

### Accumulate Unknown Values As `additional_properties`

Accumulate values that are not present in the destination BigQuery table
schema and inject as a JSON string into the payload as `additional_properties`.
This should make it possible to backfill a new column by using JSON operators
in the case that a new field was added to a ping in the client before being
added to the relevant JSON schema.

Unexpected fields should never cause the message to fail insertion.

### Errors

Send all messages that trigger an error described below to the error output.

Handle any exceptions when routing and decoding messages by returning them in a
separate `PCollection`. We detect messages that are too large to send to
BigQuery and route them to error output by raising a `PayloadTooLarge` exception.

Errors when writing to BigQuery via streaming inserts are returned as a
`PCollection` via the `getFailedInserts` method.
Use `InsertRetryPolicy.retryTransientErrors` when writing to BigQuery so that
retries are handled automatically and all errors returned are non-transient.

#### Error Message Schema

Always include the error attributes specified in the [Decoded Error Message
Schema](decoder_service_specification.md#error-message-schema).

Encode errors received as type `TableRow` as JSON in the payload of a
`PubsubMessage`, and add error attributes.

Do not modify errors received as type `PubsubMessage` except to add error
attributes.

## Other Considerations

### Message Acks

Acknowledge messages in the PubSub topic subscription only after successful
delivery to an output. Only deliver messages to a single output.
