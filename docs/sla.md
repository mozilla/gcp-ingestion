# Overview

This document describes a prototype for how Downtime on GCP Ingestion will be
measured and handled.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Disclaimer and Purpose](#disclaimer-and-purpose)
- [Service Level Agreement (SLA)](#service-level-agreement-sla)
  - [Service Level Objective](#service-level-objective)
  - [Definitions](#definitions)
  - [Exclusions](#exclusions)
  - [Additional Information](#additional-information)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Disclaimer and Purpose

**This agreement is intended solely for those directly running, writing, and
managing GCP Ingestion. It is not an agreement, implicit or otherwise, with any
other parties.** This agreement is a prototype that should be treated as a goal
that will require adjustments.

The purpose of this agreement is first and foremost to encourage behavior that
reduces Downtime. The secondary purpose is to establish clear expectations for
how to respond to Downtime and how much Downtime is considered acceptable.

# Service Level Agreement (SLA)

In production the Covered Service will provide a minimum Biweekly Uptime
Percentage determined by the Service Level Objective (SLO). If the Covered
Service does not meet the SLO then a Stability Work Period should be assigned
to engineers supporting the Covered Service.

## Service Level Objective

| Covered Service | Biweekly Uptime Percentage |
|-----------------|----------------------------|
| ingestion-edge  | 99.99%                     |
| ingestion-beam  | 99.5%                      |

## Definitions

**"Covered Service"** means ingestion-edge and ingestion-beam jobs.

**"Downtime"** on ingestion-edge means for at least 0.1% of requests [the
spec](edge.md) was not followed or a status code lower than 500 was not
successfully returned. On ingestion-beam it means the spec was not followed for
at least 0.1% of messages or `oldest_unacked_message_age` on a PubSub input
exceeds 1 hour for a batch sink job or 1 minute for a decoder or streaming sink
job.

**"Downtime Period"** means a period of 60 consecutive seconds of Downtime.
Intermittent Downtime for a period of less than 60 consecutive seconds will not
be counted towards any Downtime Periods.

**"Biweekly Uptime Percentage"** means total number of minutes in a rolling two
week window, minus the number of minutes of Downtime suffered from all Downtime
Periods in the window, divided by the total number of minutes in the window.

**"Stability Work"** means work on preventing downtime while all other features
and work are suspended.

**"Stability Work Period"** means a period of time spent on Stability Work
determined below.

| Covered Service | Biweekly Uptime Percentage | Time spent on Stability Work |
|-----------------|----------------------------|------------------------------|
| ingestion-edge  | 99.9% to < 99.99%          | 2 weeks                      |
| ingestion-edge  | 99% to < 99.9%             | 4 weeks                      |
| ingestion-edge  | < 99%                      | 3 months                     |
| ingestion-beam  | 99% to < 99.5%             | 2 weeks                      |
| ingestion-beam  | 95% to < 99%               | 4 weeks                      |
| ingestion-beam  | < 95%                      | 3 months                     |

## Exclusions

The SLA does not apply to any Downtime on a Covered Service caused by Downtime
on a Google Cloud Platform Service, other than Downtime on ingestion-edge
caused by PubSub.

## Additional Information

| Biweekly Uptime Percentage | Downtime per Two Weeks |
|----------------------------|------------------------|
| 99.99%                     | 2 minutes              |
| 99.9%                      | 20 minutes             |
| 99.5%                      | 1 hour 40 minutes      |
| 99%                        | 3 hours 21 minutes     |
| 95%                        | 16 hours 48 minutes    |
