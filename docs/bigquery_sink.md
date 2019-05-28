# BigQuery Sink Service Specification

This document specifies the behavior of the service that delivers decoded
messages into BigQuery.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Data Flow](#data-flow)
  - [Implementation](#implementation)
  - [Configuration](#configuration)
  - [Routing](#routing)
  - [Ignore Unknown Values](#ignore-unknown-values)
  - [Errors](#errors)
    - [Error Message Schema](#error-message-schema)
- [Other Considerations](#other-considerations)
  - [Message Acks](#message-acks)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

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
   `FILE_LOADS`

### Coerce Types

In some cases, we can not perfectly represent a given ping's schema in BigQuery
and we must make adjustments. The adjustments are codified in
[jsonschema-transpiler](https://github.com/mozilla/jsonschema-transpiler)
and we will preprocess the JSON payload in each message to match the schemas
found in BigQuery. Some of the supported logical transformations are:

- Map types will be transformed to arrays of key/value structs
- Complex types will be transformed to JSON strings when the destination field
  in BigQuery expects a string

### Accumulate Unknown Values As `additional_properties`

While traversing the payload in order to coerce types, we also remove any
fields from the payload that have no corresponding field in BigQuery.
We accumulate all those removed values, convert them to a JSON string, and
add them back to the payload as `additional_properties`.
This should make it possible to backfill a new column by using JSON operators
in the case that a new field was added to a ping in the client before being
added to the relvant JSON schema.

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
Schema](decoder.md#error-message-schema).

Encode errors received as type `TableRow` as JSON in the payload of a
`PubsubMessage`, and add error attributes.

Do not modify errors received as type `PubsubMessage` except to add error
attributes.

## Other Considerations

### Message Acks

Acknowledge messages in the PubSub topic subscription only after successful
delivery to an output. Only deliver messages to a single output.
