# Reliability

In production the ingestion product aims to provide a Biweekly Uptime
Percentage determined by the Reliability Target below. If a component does
not meet that then a Stability Work Period should be assigned
to each software engineer supporting the component.

## Disclaimer and Purpose

**This document is intended solely for those directly running, writing, and
managing GCP Ingestion. It is not an agreement, implicit or otherwise, with any
other parties.** This document is a prototype that should be treated as a goal
that will require adjustments.

The purpose of this document is first and foremost to encourage behavior that
reduces Downtime. The secondary purpose is to establish clear expectations for
how software engineers respond to Downtime.

## Reliability Target

| Component      | Biweekly Uptime Percentage |
|----------------|----------------------------|
| ingestion-edge | 99.99%                     |
| ingestion-beam | 99.5%                      |

## Definitions

**"Downtime"** on ingestion-edge means for at least 0.1% of requests a status
code lower than 500 was not successfully returned. On ingestion-beam it means
`oldest_unacked_message_age` on a PubSub input exceeds 1 hour for a batch sink
job or 1 minute for a decoder or streaming sink job.

**"Downtime Period"** means a period of 60 consecutive seconds of Downtime.
Intermittent Downtime for a period of less than 60 consecutive seconds will not
be counted towards any Downtime Periods.

**"Biweekly Uptime Percentage"** means total number of minutes in a rolling two
week window, minus the number of minutes of Downtime suffered from all Downtime
Periods in the window, divided by the total number of minutes in the window.

**"Stability Work"** means work on Downtime prevention and reduction. Only
code changes resulting from that work may be deployed to production. All other
features and work on the component are suspended.

**"Stability Work Period"** means a continuous period of time after recovery
and postmortem, where each engineer is assigned Stability Work. The length of
the Stability Work Period is determined below.

| Component      | Biweekly Uptime Percentage | Length of Stability Work Period |
|----------------|----------------------------|---------------------------------|
| ingestion-edge | 99.9% to < 99.99%          | 2 weeks                         |
| ingestion-edge | 99% to < 99.9%             | 4 weeks                         |
| ingestion-edge | < 99%                      | 12 weeks                        |
| ingestion-beam | 99% to < 99.5%             | 2 weeks                         |
| ingestion-beam | 95% to < 99%               | 4 weeks                         |
| ingestion-beam | < 95%                      | 12 weeks                        |

## Exclusions

The Reliability Target does not apply to any Downtime in ingestion caused by
Downtime on a Google Cloud Platform Service, other than Downtime on
ingestion-edge caused by PubSub.

## Additional Information

| Biweekly Uptime Percentage | Downtime per Two Weeks |
|----------------------------|------------------------|
| 99.99%                     | 2 minutes              |
| 99.9%                      | 20 minutes             |
| 99.5%                      | 1 hour 40 minutes      |
| 99%                        | 3 hours 21 minutes     |
| 95%                        | 16 hours 48 minutes    |
