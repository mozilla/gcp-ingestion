# Overview

A running list of things that are suboptimal in GCP.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [App Engine](#app-engine)
- [Dataflow](#dataflow)
  - [`BigQueryIO.Write`](#bigqueryiowrite)
  - [`FileIO.Write`](#fileiowrite)
  - [`PubsubIO.Write`](#pubsubiowrite)
- [PubSub](#pubsub)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# App Engine

For network-bound applications it can be prohibitively expensive. A PubSub push
subscription application that decodes protobuf and forwards messages to the
ingestion-edge used `~300` instances at `$0.06` per instance hour to handle
`~5krps`, which is `~$13K/mo`.

# Dataflow

Replaces certain components with custom behavior that is not part of the open
source Beam API, making it so they can't be extended (e.g. to expose a stream
of messages that have been delivered to PubSub).

## `BigQueryIO.Write`

Requires decoding `PubsubMessage.payload` from JSON to a `TableRow`, which gets
encoded as JSON to be sent to BigQuery.

Crashes the pipeline when the destination table does not exist.

## `FileIO.Write`

Acknowledges messages in PubSub before they are written due to a required
`GroupByKey` transform. This also effects `BigQueryIO.Write` in batch mode.

## `PubsubIO.Write`

Does not support dynamic destinations.

Does not use standard client library.

Does not expose an output of delivered messages, which is needed for at least
once delivery with deduplication. Current workaround is to get delivered
messages from a subscription to the output PubSub topic.

Uses HTTPS JSON API, which increases message payload size vs protobuf by 25%
for base64 encoding and causes some messages to exceed the 10MB request size
limit that otherwise would not.

# PubSub

Can be prohibitively expensive. It costs
[`~$51K/mo`](https://cloud.google.com/products/calculator/#id=9bb92e31-ea3f-475b-afff-52da0796e0a7)
to use PubSub with a `70MiB/s` stream published or consumed 7 times (Edge to
raw topic, raw topic to Cloud Storage, raw topic to Decoder, Decoder to decoded
topic, decoded topic to Decoder for deduplication, decoded topic to Cloud
Storage, decoded topic to BigQuery).

Push Subscriptions are limited to `min(10MB, 1000 messages)` in flight, making
the theoretical maximum parallel latency per message ~`62ms` to achieve
`16krps`.
