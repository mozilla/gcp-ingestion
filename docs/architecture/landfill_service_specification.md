# Landfill Service Specification

This document specifies the behavior of the service that batches raw messages
into long term storage.



## Data Flow

Consume messages from a Google Cloud PubSub topic and write in batches to
Google Cloud Storage. Split batches based by time windows based on when they
were retrieved from PubSub. Additionally, split batches when they reach a
certain size, if possible.

### Implementation

Execute this as an Apache Beam job.

### Latency

Accept a configuration for batch window size. Deliver batches to Cloud Storage
within 5 minutes of the batch window closing.

## Other Considerations

### Message Acks

Only acknowledge messages in the PubSub topic subscription after delivery to
Cloud Storage. This is the default behavior for Beam as long as no shuffle
operations are performed. For very long windows Beam should automatically
extend the ack deadline of undelivered messages.
