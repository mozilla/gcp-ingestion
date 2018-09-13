[![CircleCI](https://circleci.com/gh/mozilla/gcp-ingestion.svg?style=svg&circle-token=d98a470269580907d5c6d74d0e67612834a21be7)](https://circleci.com/gh/mozilla/gcp-ingestion)

# Ingestion Edge Server

A simple service for delivering HTTP messages to Google Cloud PubSub

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


  - [Building](#building)
  - [Running](#running)
  - [Testing](#testing)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Building

```bash
docker build -t mozilla/ingestion-edge:build .
```

## Running

To run the server locally:

```bash
# create network edge to communicate with pubsub
docker network create ingestion-edge

# run the pubsub emulator
PUBSUB_PORT=8085
docker run --detach --network ingestion-edge --name pubsub google/cloud-sdk gcloud beta emulators pubsub start --host-port 0.0.0.0:$PUBSUB_PORT

# run the server
docker run --rm --tty --interactive --network ingestion-edge --publish 8000:8000 --env PUBSUB_EMULATOR_HOST=pubsub:$PUBSUB_PORT --env ROUTE_TABLE='[["/submit/<path:suffix>","projects/test/topics/test"]]' mozilla/ingestion-edge:build

# manually check the server
curl http://localhost:8000/__version__
curl http://localhost:8000/__heartbeat__
curl http://localhost:8000/__lbheartbeat__
curl http://localhost:8000/submit/test -d "test"
```

## Testing

Run tests locally with [CircleCI Local CLI](https://circleci.com/docs/2.0/local-cli/#installing-the-circleci-local-cli-on-macos-and-linux-distros)

```bash
circleci build --job ingestion-edge
```

Test a remote server from docker (requires credentials to read PubSub)

```bash
# define the same ROUTE_TABLE as your edge server
docker run --rm --tty --interactive --env ROUTE_TABLE=$ROUTE_TABLE mozilla/ingestion-edge:build py.test --server https://myedgeserver.example.com
```

# License

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
