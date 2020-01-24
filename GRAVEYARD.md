# Link to code removed from this repository

- 2020-01-24 (available until [commit `d6f4f25`](https://github.com/mozilla/gcp-ingestion/tree/d6f4f2528019bba5ff1805b17c95785a1770f223))
  - Remove patched `WritePartition.java` that limits the maximum number of bytes
    in a single BigQuery load job; the patch only affects large backfill jobs
    and there is a new configuration parameter we can use in Beam 2.19 that we can use
    to get this same effect. We're removing this as part of upgrading to Beam 2.18
    because `WritePartition.java` changes in that version and it's risky to have
    to rewrite our patch.
- 2020-01-24 (available until [commit `16d7702`](https://github.com/mozilla/gcp-ingestion/tree/16d770233c073af07c9b0f7ca6f9a1b4080d71d3))
  - `HekaReader` and the `heka` input type were removed
  - The `sanitizedLandfill` input format was removed along with AWS dependencies
