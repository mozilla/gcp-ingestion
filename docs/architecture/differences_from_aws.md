# Differences from AWS

This document explains how GCP Ingestion differs from the [AWS Data Platform
Architecture](https://mana.mozilla.org/wiki/display/SVCOPS/Telemetry+-+Data+Pipeline+Architecture).

## Replace Heka Framed Protobuf with newline delimited JSON

Heka framed protobuf requires special code to read and write. Newline delimited
JSON is readable by BigQuery, Dataflow, and Spark using standard libraries. JSON
doesn't enforce a schema, so it can be used to store data with an incomplete
schema and be used to backfill missing columns.

## Replace EC2 Edge with Kubernetes Edge

The AWS data platform uses EC2 instances running an NGinX module to encode HTTP
requests as Heka messages and then write them to Kafka using `librdkafka` and
directly to files on disk. `librdkafka` handles buffering and batching when
writing to Kafka. Files on disk are rotated with cron and uploaded to S3. On
shutdown files are forcefully rotated and uploaded. The sizing of the EC2
instance cluster is effectively static, but is configured to scale up if
needed.

The EC2 instances have been replaced with a Kubernetes cluster. This decision
was made by the Cloud Operations team to simplify operational support for them.

The NGinX module has been replaced by an HTTP service running in Docker. A
number of factors informed the decision to rewrite the edge, including:

- The PubSub equivalent of `librdkafka` is the [google client
  libraries](https://cloud.google.com/apis/docs/cloud-client-libraries),
  which do not have a C implementation
- We can simplify the edge by uploading to landfill after PubSub while
  remaining resilient to Dataflow and PubSub failures, because PubSub durably
  stores unacknowledged messages for 7 days
- We can simplify disaster recovery by Ensuring that all data eventually flows
  through PubSub
   - In the AWS edge data only flows to at least one of Kafka or landfill
- We can allow Kubernetes to auto scale when PubSub is available by only
  queuing requests to disk only when they cannot be delivered to PubSub
- We can ensure that data is not lost on shutdown by disabling auto scaling
  down when there are requests on disk

## Replace Kafka with PubSub

Comparison:

|                | Kafka in AWS Data Pipeline        | PubSub                      |
|----------------|-----------------------------------|-----------------------------|
| Managed by     | Ops                               | Google                      |
| Access control | Security groups, all-or-nothing   | Cloud IAM, per-topic        |
| Scaling        | Manual                            | Automatic                   |
| Cost           | Per EC2 instance                  | Per GB, min charge 1 KB/req |
| Data Storage   | Configured in GB per EC2 instance | 7 days for unacknowledged   |
| Cross region   | no                                | yes                         |

## Replace Hindsight Data Warehouse Loaders with Dataflow

Dataflow advantages:

- Connectors for PubSub, Cloud Storage, and BigQuery built-in
- Seamlessly supports streaming and batch sources and sinks
- Runs on managed service and has simple local runner for testing and
  development
- Auto scales on input volume

## Replace S3 with Cloud Storage

They are equivalent products for the different cloud vendors.

## Messages Always Delivered to Message Queue

In AWS the edge aimed to ensure messages were always delivered to either Kafka
or landfill, and in the case of an outage one could be backfilled from the
other. On GCP the Kubernetes edge aims to ensure messages are always delivered
to PubSub. This ensures that consumers from PubSub never miss data unless they
fall too far behind. It also allows landfill to be handled downstream from
PubSub (see below).

## Landfill is Downstream from Message Queue

In AWS, the failsafe data store was upstream of the message queue (Kafka). On
GCP, the failsafe data store is downstream from the message queue (PubSub).

This makes the edge and Dataflow landfill loader simpler. The edge doesn’t have
to ensure that pending messages are safely offloaded on shutdown, because
messages are only left pending when PubSub is unavailable, and scaling down is
disabled while messages are pending. The Dataflow landfill loader doesn’t have
to ensure pending messages are safely offloaded because it only acks messages
after they are uploaded, ensuring at least once delivery.

This design is possible because of two changes compared to our AWS
implementation. First, the Kubernetes edge eventually delivers all messages to
PubSub. In AWS if Kafka were down then messages would only be delivered
directly to landfill, and would never flow through Kafka. This change ensures
that if all messages are consumed from PubSub then no messages have been
skipped. Second, PubSub stores unacknowledged messages for 7 days. In AWS Kafka
stores messages for 2-3 days, depending on topic and total message volume. This
change ensures that we have sufficient time to reliably consume all messages
before they are dropped from the queue, even if total message volume changes
dramatically or consumers are not highly available and suffer an outage over a
holiday weekend.
