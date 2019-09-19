# Test Requirements

This document specifies the testing required for GCP Ingestion components.



## Exceptions

Code that does not comply with this standard before it is deployed to
production must include a document explaining why that decision was made.

## Test Phases

1. Continuous Integration
   - Must run [Unit Tests](#unit-tests) and [Integration Tests](#integration-tests)
   - Must run against every merge to master and every pull request
   - Must block merging to master
1. Pre-Production
   - Must run [Unit Tests](#unit-tests) and [Load Tests](#load-tests)
   - Must run against every version deployed to production
   - Must mimic production configuration as closely as possible
   - Should block deployment to production
1. Async
   - Must run [Slow Load Tests](#slow-load-tests)
   - Must run against every version deployed to production
   - Must mimic production configuration as closely as possible
   - Should not block deployment to production
   - Must notify someone to investigate failures

## Test Categories

Tests must be both thorough and fast enough to not block development. They are
split into categories with increasing run time and decreasing coverage.

1. [Unit Tests](#unit-tests)
   - Must cover 100% of code behavior
   - Should run as fast as possible
1. [Integration Tests](#integration-tests)
   - Must cover all expected production behavior
   - Must run fast enough to allow frequent merging to master
1. [Load Tests](#load-tests)
   - Must cover performance at scale with and without external services down
   - Must run fast enough to allow multiple production deployments per day
1. [Slow Load Tests](#slow-load-tests)
   - Must cover performance at scale with extended downtime
   - May take a very long time

### Unit Tests

- Must run in under 5 seconds in CI, not including build time
   - May use CI parallelism and run each group of tests in under 5 seconds in CI
   - Should validate code as quickly as possible
- Must run completely in memory
   - Must safely run in parallel
   - May mock anything that would not run in memory
- Should cover 100% of branches and statements
   - Should cover all variations of statements having variable behavior without
     branching
   - Should cover all production configuration variations
   - Must have [Integration Tests](#integration-tests) to cover exceptions
- Must cover all input and output variations
   - Must cover all response codes
   - Must cover all valid URL patterns
   - Must cover gzipped and not gzipped
   - Must cover present, absent, and invalid required attributes
   - Must cover present, absent, and invalid optional attributes
   - Must cover present, absent, invalid, and wrong type payload JSON
   - Must cover present, absent, invalid, and wrong type payload fields
   - Must cover `document_id` duplicates
- Must cover all error types
   - Must cover PubSub returning OK but rejecting messages in a batch
   - Must cover External service returns server error
   - Must cover External service timeout
   - Must cover External resource missing
   - Must cover Insufficient permissions
   - Must cover behavior when disk becomes full
   - Must cover behavior with disk already full
- Must cover behavior when ingestion-edge disk is no longer full

### Integration Tests

- Must run in under 5 minutes in CI, not including build time
- Must be configurable to run locally and in CI
- Must be configurable to run against a deployment in GCP
   - May require a proxy to modify cloud service behavior
   - May require specific configuration
   - May skip coverage that cannot be forcibly produced in Dataflow
- Must clean up created resources when complete
- Must cover everything unit tests should but do not
- Must cover a configuration that mimics production as closely as possible
- Must cover all code with input from or output to external services
   - The ingestion-edge disk queue is an external service
- Must cover all production configuration options
- Must cover all API endpoints with invalid traffic
- Must cover all API endpoints with healthy external services
   - Must assert disk queue is not used in ingestion-edge
   - Must assert inputs and outputs preserve schema in ingestion-beam
- Must cover behavior with errors from external services
   - Must cover PubSub returning OK but rejecting messages in a batch
   - Must cover external service DNS does not resolve
   - Must cover external service cannot TCP connect
   - Must cover external service hangs after TCP connect
   - Must cover external service returns 500
   - Must cover resource does not exist in external service
   - Must cover insufficient permissions in external service
   - Must cover behavior when disk becomes full
   - Must cover behavior with disk already full
   - Must assert automatic recovery unless manual intervention is required
- Must cover behavior when ingestion-edge disk is no longer full

### Load Tests

- Must run in parallel in under 1 hour
   - May require a separate deployment for each test
- Must record how much it cost to run
- Must not require manual intervention to evaluate
- Must check performance against a tunable threshold
- Must sustain at least 1.5x expected peak production traffic volume
   - As of 2018-08-01 peak production traffic volume is about 11K req/s
- Must cover behavior before, during, and after a period of:
   - Service downtime, except ingestion-edge
   - 100%, 50%, and 5% invalid traffic
   - External services cannot TCP connect
   - 100% and 10% of external service requests time out
   - 100% and 10% external service requests return 500
- Must cover ingestion-edge behavior with PubSub returning 500 and auto scaling
  due to disk filling up
- Must assert ingestion-edge only writes to disk queue when PubSub is unhealthy
- Must assert full recovery from downtime does not take longer than the
  downtime period

### Slow Load Tests

- May run for longer than 1 hour
- Should not block deployment to production
- Must cover behavior with sustained load and a backlog equivalent to:
   - For ingestion-edge: 1.5x the longest PubSub outage in the last year
      - As of 2018-08-01 the longest PubSub outage was about 4.5 hours
   - For everything else: 4 days (1 holiday weekend)
      - Should reach full recovery in under 1 day
- Must cover behavior of ingestion-beam BigQuery output with sustained load and
  BigQuery returning 500 for at least 1.5x the longest BigQuery outage in the
  last year
   - As of 2018-08-01 the longest BigQuery outage was about 2.3 hours
