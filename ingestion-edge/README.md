[![CircleCI](https://circleci.com/gh/mozilla/gcp-ingestion.svg?style=svg&circle-token=d98a470269580907d5c6d74d0e67612834a21be7)](https://circleci.com/gh/mozilla/gcp-ingestion)

# Ingestion Edge Server

A simple service for delivering HTTP messages to Google Cloud PubSub

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


  - [Building](#building)
  - [Running](#running)
  - [Configuration](#configuration)
  - [Testing](#testing)
    - [Style Checks](#style-checks)
    - [Unit Tests](#unit-tests)
    - [Integration Tests](#integration-tests)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Building

```bash
docker-compose build
```

## Running

Use `docker-compose` to run a local development server that auto-detects changes:

```bash
# run the web server and pubsub emulator
docker-compose up --detach web

# manually check the server
curl http://localhost:8000/__version__
curl http://localhost:8000/__heartbeat__
curl http://localhost:8000/__lbheartbeat__
curl http://localhost:8000/submit/test -d "test"

# check web logs
docker-compose logs web

# clean up docker-compose environment
docker-compose down --timeout 0
```

## Configuration

The ingestion-edge docker container accepts these configuration options from
environment variables:

- `ROUTE_TABLE`: a JSON list of mappings from `uri` to PubSub topic, defaults
  to `[]`, each mapping is a list and may include an optional third element
  that specifies a list of allowed methods instead of the default
  `["POST","PUT"]`
- `QUEUE_PATH`: a filesystem path to a directory where a SQLite database will
  be created to store requests for when PubSub is unavailable, paths may be
  relative to the docker container `WORKDIR`, defaults to `queue`
- `MINIMUM_DISK_FREE_BYTES`: an integer indicating the threshold of free bytes
  on the filesystem where `QUEUE_PATH` is mounted below which `/__heartbeat__`
  will fail, defaults to `0` which disables the check
- `METADATA_HEADERS`: a JSON list of headers to preserve as PubSub message
  attributes, defaults to `["Content-Length", "Date", "DNT", "User-Agent",
  "X-Forwarded-For", "X-Pingsender-Version", "X-Pipeline-Proxy"]`
- `PUBLISH_TIMEOUT_SECONDS`: a float indicating the maximum number of seconds
  to wait for the PubSub client to complete a publish operation during an HTTP
  request, defaults to 1 second and may require tuning
- `FLUSH_PUBLISH_TIMEOUT_SECONDS`: a float indicating the maximum number of
  seconds to wait for the PubSub client to complete a publish operation when
  flushing, defaults to `PUBLISH_TIMEOUT_SECONDS`
- `FLUSH_CONCURRENT_MESSAGES`: an integer indicating the number of messages per
  worker that may be read from the queue before waiting on publish results,
  defaults to 1000 messages based on [publish request
  limits](https://cloud.google.com/pubsub/quotas#resource_limits) and may
  require tuning
- `FLUSH_CONCURRENT_BYTES`: an integer indicating the number of bytes per
  worker that may be read from the queue before waiting on publish results,
  which may be exceeded by one message and measures data bytes rather than
  serialized message size, defaults to 10MB based on [publish request
  limits](https://cloud.google.com/pubsub/quotas#resource_limits) and may
  require tuning
- `FLUSH_SLEEP_SECONDS`: a float indicating the number of seconds waited
  between flush attempts, defaults to 1 second and may require tuning

## Testing

Run all tests locally with `docker-compose` or
[CircleCI Local CLI](https://circleci.com/docs/2.0/local-cli/#installing-the-circleci-local-cli-on-macos-and-linux-distros)

```bash
# only print test logs and leave other services running
docker-compose run --rm test

# rebuild images, recreate containers, stop all containers when one exits, use SIGKILL immediately
docker-compose up --build --force-recreate --abort-on-container-exit --timeout=0

# circleci
(cd .. && circleci build --job ingestion-edge)
```

### Style Checks

Update dependencies installed in the container as needed

```bash
docker-compose build
```

Run style checks

```bash
docker-compose run --rm --no-deps test --black --docstyle --flake8 --mypy --mypy-ignore-missing-imports ingestion_edge
```

### Unit Tests

Update dependencies installed in the container as needed

```bash
docker-compose build
```

Run unit tests

```bash
docker-compose run --rm --no-deps test tests/unit
```

### Integration Tests

Update dependencies installed in the container as needed

```bash
docker-compose build
```

Run integration tests locally

```bash
docker-compose run --rm test tests/integration --server http://web:8000
```

Test a remote server (requires credentials to read PubSub)

```bash
# define the same ROUTE_TABLE as your edge server
docker-compose run --no-deps --rm --env ROUTE_TABLE=$ROUTE_TABLE test --server https://myedgeserver.example.com

# or using the latest published version without a code checkout
docker run --rm --tty --interactive --env ROUTE_TABLE=$ROUTE_TABLE mozilla/ingestion-edge:latest py.test --server https://myedgeserver.example.com
```

# License

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
