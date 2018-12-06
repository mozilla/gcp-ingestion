# Overview

A running list of things that are suboptimal in GCP.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [PubSub Push Subscriptions](#pubsub-push-subscriptions)
- [Dataflow `PubsubIO.Write`](#dataflow-pubsubiowrite)
- [App Engine](#app-engine)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# PubSub Push Subscriptions

Limited to `min(10MB, 1000 messages)` in flight, making the theoretical maximum
parallel latency per message ~`62ms` to achieve `16krps`.

# Dataflow `PubsubIO.Write`

Does not use standard client library.
Does not expose an output of delivered messages.

# App Engine

For network-bound applications it can be prohibitively expensive. A PubSub push
subscription application that decodes protobuf and forwards messages to the
ingestion-edge used `~300` instances at `$0.06` per instance hour to handle
`~5krps`, which is `~$13K/mo`.
