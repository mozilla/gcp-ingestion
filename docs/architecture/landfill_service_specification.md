# Raw Sink Service Specification

This document specifies the behavior of the service that batches raw messages
into long term storage.

## Data Flow

Consume messages from a Google Cloud PubSub topic and write in batches to
BigQuery. Split batches based by time windows based on when they
were retrieved from PubSub. Additionally, split batches when they reach a
certain size, if possible.

### Implementation

Execute this as a custom Java application running on GKE.

### Latency

Accept a configuration for batch window size. Deliver batches to BigQuery
within 5 minutes of the batch window closing.

## Other Considerations

### Message Acks

Only acknowledge messages in the PubSub topic subscription after delivery to
BigQuery.
