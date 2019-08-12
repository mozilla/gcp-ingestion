# Pain points

A running list of things that are suboptimal in GCP.

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

Acknowledges messages in PubSub before they are written to accumulate data
across multiple [bundles] and produce reasonably sized files. Possible
workaround being investigated in [#380]. This also effects `BigQueryIO.Write`
in batch mode.

[bundles]: https://beam.apache.org/documentation/execution-model/#bundling-and-persistence
[#380]: https://github.com/mozilla/gcp-ingestion/issues/380

## `PubsubIO.Write`

Does not support dynamic destinations.

Does not support [`NestedValueProvider`] for destinations in streaming mode on
Dataflow, which is needed to create templates that accept a mapping of document
type to a predetermined number of destinations. This is because Dataflow moves
the implementation into the shuffler to improve performance. Current workaround
is to specify mapping at template creation time.

Does not use standard client library.

Does not expose an output of delivered messages, which is needed for at least
once delivery with deduplication. Current workaround is to get delivered
messages from a subscription to the output PubSub topic.

Uses HTTPS JSON API, which increases message payload size vs protobuf by 25%
for base64 encoding and causes some messages to exceed the 10MB request size
limit that otherwise would not.

[`NestedValueProvider`]: https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/options/ValueProvider.NestedValueProvider.html

## Templates

Does not support repeated parameters via `ValueProvider<List<...>>`, as
described in [Dataflow Java SDK #632]

[GoogleCloudPlatform/DataflowJavaSDK#632]: https://github.com/GoogleCloudPlatform/DataflowJavaSDK/issues/632

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
