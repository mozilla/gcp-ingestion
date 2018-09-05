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

## Testing

Always have 100% branch and statement test coverage for code that is in
production. If a statement has variable behavior without branching, test all
behavior variations.

### CI Testing

Run CI tests for all of the behavior described in [Data Flow](#data-flow)
including, but not limited to:

 * Handling messages with and without `attributes`
 * Storing the exact same PubSub schema received

### Pre-deployment Testing

Always execute full coverage testing with actual Google Cloud services
before deploying to production.

Additionally run the following tests:

 * Load test with 1.5x peak production traffic volume over the last month. As
   of 2018-08-01 peak production traffic volume is about 11K req/s.
   * Impose a time limit to ensure that we are meeting the requirements laid
     out in [Latency](#latency).
 * Test behavior under all failure conditions that can be simulated or forcibly
   produced when running in Dataflow. Possible failure conditions we may be
   simulate or produce include: PubSub outage, Dataflow full disk, and Cloud
   Storage outage.
