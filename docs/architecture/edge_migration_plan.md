# Plans for migrating edge traffic to GCP Ingestion

This document outlines plans to migrate edge traffic from AWS to GCP using the code in this repository.



## Current state

Today, data producers send data to the ingestion stack on AWS as described [here](https://github.com/mozilla/firefox-data-docs/blob/042fddcbf27aa5993ee5578224200a3ef65fd7c7/src/concepts/pipeline/data_pipeline_detail.md#ingestion).

## Phase 1

Timeline: **Q4 2018**

```
Data Producers -> AWS Edge -> Kafka -> AWS Hindsight -> PubSub -> GCP Hindsight w/ HTTP Output -> GCP Edge
```

In this configuration we get 100% of "expected" data going to the GCP pipeline without any risk of affecting production AWS processing.

This "expected" data does _not_ include recording and replaying traffic that we currently throw away at the edge (e.g. data from Firefox versions prior to unified telemetry). There may be some other subtle differences from the original incoming data from producers, such as missing some headers that are not currently being stored in Landfill. On the whole, this is a good approximation of 100% of the data we actually care about. We also need to test operation of the new system while processing data we _don't_ care about; see more detail in Phase 3 below.

During this phase, we will collect logging info to determine exactly what data is being ignored by limiting to this "expected" data.

Why have a GCP PubSub topic instead of running an HTTP output from the AWS consumer directly? The main reason is that we want 100% of data in a PubSub topic anyway, for staging purposes. This way we can have a production GCP Edge ingestion stack writing to production GCP resources, while being able to e.g. simulate load and various outage conditions using the same data in a staging environment. We could in theory do the stage testing using the prod GCP edge output topic, assuming the edge has no issues. This will be the eventual end state of the system when there's no more AWS, but we are currently reusing most of the hindsight tooling for stage testing since it's already written and working in production.

## Phase 2

Timeline: **possibly Q4 2018**

```
Data Producers -> AWS Edge -> Kafka -> AWS Hindsight w/ HTTP Output -> GCP Edge
```

We continue to write the Hindsight PubSub topic from Phase 1, but we move the HTTP output to the AWS side. This will help us better empirically determine performance and various other implications of cross-cloud requests without potentially affecting production AWS processing. We can still use the GCP PubSub topic for stage testing, but it won't necessarily be actively used and the production GCP Edge will be receiving its data directly from AWS via HTTP POST.

## Phase 3

Timeline: **2019**

```
Data Producers -> AWS Tee -> AWS Edge
                          \
                           `-> GCP Edge
```

This is how we did the last major migration from [`Heka`](https://hekad.readthedocs.io/en/v0.10.0/) to [`Hindsight`](http://mozilla-services.github.io/hindsight/).

This architecture introduces risk of data loss, so should be considered more dangerous than previous phases. This is why we are not planning on doing this until we're reasonably confident in the efficacy of the GCP infrastructure. In active tee mode, the client will be affected by GCP edge processing, particularly in request processing time and potentially by its status code. Depending on how we configure the tee, the AWS data ingestion infrastructure is susceptible to data duplication or loss due to client retry behavior. 

We should ensure that we are sufficiently confident in the behavior, performance, and stability of the GCP Edge before we move to this phase to ensure things don't go south. The previous phases are safer and should let us discover any major issues before we proceed to this phase.

This does the "last mile" testing of most of the major missing pieces from earlier phases, in addition to prepping for the eventual fourth phase.

## Phase 3 (alternative)

```
Data Producers -> Weighted DNS -> AWS Edge
                               \
                                `-> GCP edge
```

This alternative does not make use of a Tee to duplicate traffic.

Depending on the results of Phase 2, an alternative strategy to teeing would be weighted DNS and running two GCP edges. In this configuration we could e.g. tee 1% of data directly to one of the GCP edges, while having the other 99% be processed by the other edge as per earlier phases. We would then need to do the reverse of Phase 1 and write that 1% back to AWS + Kafka for the AWS ingestion stack to process. This strategy can be just as dangerous as using the `OpenResty` tee because data loss on either side may result in partial data to both, and also requires writing code to convert the GCP representation back to the AWS representation. The risk can be mitigated by using the DNS weights to adjust the amount of data being sent directly to GCP, which is an advantage. This is an interesting variation more similar to a standard DNS cut-over, if required.

## Phase 4

Timeline: **To Be Determined**

```
Data Producers -> GCP Edge
```

This will happen after we are confident that there is no risk of data loss by switching the endpoint to the GCP stack. It will also depend on the logistics and timing of sunsetting systems and components in AWS.
