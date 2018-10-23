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
 * The output mode for BigQuery, default to `STREAMING_INSERTS`
 * The triggering frequency for writing to BigQuery, when output mode is
   `FILE_LOADS`

### Routing

Send messages to the fallback PubSub topic if they have no route configured.
and add the additional attributes specified in [Decoded Error Message Schema
](decoder.md#error-message-schema). If no fallback PubSub topic has been
specified, drop the message.

### Ignore Unknown Values

Specify `BigQueryIO.Write.ignoreUnknownValues()` when writing to BigQuery. This
option makes it so that fields present in a message but not in the BigQuery
schema are automatically dropped by BigQuery on insert, rather than causing the
message to fail insertion.

### Errors

Send all messages that trigger an error described below to the error output.

Handle any exceptions when routing and decoding messages by returning them in a
separate `PCollection`.

Errors when writing to BigQuery are returned as a `PCollection`
via the `getFailedInserts` method. Use `InsertRetryPolicy.retryTransientErrors`
when writing to BigQuery so that retries are handled automatically and all
errors returned are non-transient.

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
