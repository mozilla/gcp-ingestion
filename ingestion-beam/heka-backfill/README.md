# Heka Backfill

This directory codifies the steps to perform the backfill of 1 year of
Heka-framed protobuf files from object storage into BigQuery tables.
This backfill was initiated on 2019-10-17.

We have 31 projects set up (moz-fx-data-backfill-1 through 
moz-fx-data-backfill-31) so that we can manage jobs for multiple days
within a month concurrently in separate projects.

## Setup

Create a python `venv` in the `ingestion-beam` directory:

``` bash
cd gcp-ingestion/ingestion-beam
python3 -m venv venv
./venv/bin pip install google-cloud-bigquery
```

Authenticate with gcloud (assuming you are a member of data ops or data platform):

```
gcloud auth application-default login
```

Unpack the specific version of BQ schemas we're using for the backfill:

```
cd heka-backfill
tar -xzvf bq-schemas.tar.gz
```

## Launch Dataflow Jobs

So far, we have had success with running up to 8 Dataflow jobs concurrently.

To launch jobs for 2019-08-01 through 2019-08-08, do:

``` bash
for x in {1..8}; do 
  YEARNUM=2019 MONTHNUM=8 DAYNUM=$x ./heka-backfill/launch-dataflow-job
done
```

These should run 

## Monitor Dataflow Job Status

To check the current status of a pipeline (Running, Done, Failed) and
to see whether the associated live table has been populated, run:

``` bash
YEARNUM=2019 MONTHNUM=8 DAYNUM=3 ./heka-backfill/monitor-job
```

This will update every 60 seconds until you hit Ctrl-C.
It's not a wealth of information; in particular, it doesn't show how
many workers are currently being used, so you can't tell whether the job
is in the scaled-up phase or in the scaled-down-and-waiting-for-bq-load-jobs
phase. You need to access the individual job in the Dataflow UI to get that info.

You should generally expect the job to finish processing and ramp down within
90 minutes of launch, then run for another 2 to 12 hours waiting on BQ load jobs
to complete. It's fine to launch another batch of Dataflow jobs once all the
current jobs have ramped down.

To monitor many jobs, you can install tmux and
[xpanes](https://github.com/greymd/tmux-xpanes), launching a session like:

```
xpanes -c 'YEARNUM=2019 MONTHNUM=8 DAYNUM={} ./heka-backfill/monitor-job' {1..8}
```

## Copying and Deduplicating Results

This process could be improved. At the moment, I am running copy_dedupe
for several days in parallel, launching the jobs via xpanes and having each
run in the associated project for the day. Running more than about 6
concurrently seems to present a high risk of failure.

```
xpanes -c 'YEARNUM=2019 MONTHNUM=8 DAYNUM={} ./heka-backfill/copydedupe' {1..4}
```

The final results are all loaded into `backfill-test-252723:telemetry_stable`
so you can check on counts of final deduped records via:

```
SELECT
  DATE(submission_timestamp) AS date,
IF
  (EXTRACT(dayofweek
    FROM
      DATE(submission_timestamp)) IN (1,
      7),
    'Weekend',
    NULL) AS is_weekend,
  COUNT(*) AS n
FROM
  `backfill-test-252723.telemetry_stable.main_v4`
WHERE
  DATE(submission_timestamp) = '2019-08-12'
GROUP BY
  1,
  2
ORDER BY
  1
```
