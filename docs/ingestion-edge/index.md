# Ingestion Edge Server

A simple service for delivering HTTP messages to Google Cloud PubSub

## Building

Install and update dependencies as-needed

```bash
# docker-compose
docker-compose build

# pytest
bin/build
```

## Running

Use `docker-compose` to run a local development server that auto-detects changes:

```bash
# run the web server and PubSub emulator
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

- `ROUTE_TABLE`: a JSON list of mappings from `uri` to PubSub topic, `uri`
  matches are detected in order, defaults to `[]`, each mapping is a list and
  may include an optional third element that specifies a list of allowed
  methods instead of the default `["POST","PUT"]`
- `QUEUE_PATH`: a filesystem path to a directory where a SQLite database will
  be created to store requests for when PubSub is unavailable, paths may be
  relative to the docker container `WORKDIR`, defaults to `queue`
- `MINIMUM_DISK_FREE_BYTES`: an integer indicating the threshold of free bytes
  on the filesystem where `QUEUE_PATH` is mounted below which `/__heartbeat__`
  will fail, defaults to `0` which disables the check
- `METADATA_HEADERS`: a JSON list of headers to preserve as PubSub message
  attributes, defaults to `["Content-Length", "Date", "DNT", "User-Agent",
  "X-Forwarded-For", "X-Pingsender-Version", "X-Pipeline-Proxy", "X-Debug-ID"]`;
  the message attribute name will be the header name in lowercase and with `-`
  converted to `_`
- `PUBLISH_TIMEOUT_SECONDS`: a float indicating the maximum number of seconds
  to wait for the PubSub client to complete a publish operation, defaults to 1
  second and may require tuning
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

Run tests with [CircleCI Local
CLI](https://circleci.com/docs/2.0/local-cli/#installing-the-circleci-local-cli-on-macos-and-linux-distros),
`docker-compose`, or `pytest` wrappers

```bash
# circleci
(cd .. && circleci build --job ingestion-edge)

# docker-compose
docker-compose run --rm test

# pytest wrapper (pytest-all calls lint and pytest)
./bin/pytest-all
```

The `pytest` wrappers add these options via the environment:

- `CLEAN_RELOCATES` controls whether `bin/lint` and `bin/pytest` will remove
  `.pyc` files not in `venv/` that do not contain `$PWD` to prevent errors when
  switching between running in and out of docker, defaults to `true`
- `VENV` controls whether to use a python `venv` in `venv/$(uname)` in
  `bin/lint` and `bin/pytest`, and in `bin/build` to create and use that
  `venv`, defaults to `false` in `Dockerfile` and `true` otherwise

### Style Checks

Run style checks

```bash
# docker-compose
docker-compose run --rm test bin/lint

# pytest wrapper
./bin/lint
```

### Unit Tests

Run unit tests

```bash
# docker-compose
docker-compose run --rm test bin/pytest tests/unit

# pytest wrapper
./bin/pytest tests/unit
```

### Integration Tests

Run integration tests locally

```bash
# docker-compose
docker-compose run --rm test bin/pytest tests/integration

# pytest wrapper
./bin/pytest tests/integration
```

Test a remote server (requires credentials to read PubSub)

```bash
# define the same ROUTE_TABLE as your edge server
export ROUTE_TABLE='[["/submit/telemetry/<suffix:path>","projects/PROJECT/topics/TOPIC"]]'

# docker using latest image and no git checkout
docker run --rm --tty --interactive --env ROUTE_TABLE mozilla/ingestion-edge:latest bin/pytest tests/integration --server https://myedgeserver.example.com

# docker-compose
docker-compose run --rm -e ROUTE_TABLE test bin/pytest tests/integration --server https://myedgeserver.example.com

# pytest wrapper
./bin/pytest tests/integration --server https://myedgeserver.example.com
```

### Load Tests

Run a load test (defaults to a single GKE cluster and a PubSub emulator)

```bash
# docker using latest image and no git checkout
docker run --rm --tty --interactive mozilla/ingestion-edge:latest bin/pytest tests/load

# docker-compose
docker-compose run --rm test bin/pytest tests/load

# pytest
./bin/pytest tests/load
```

Load test options (from `./bin/test -h`)

```
  --min-success-rate=MIN_SUCCESS_RATE
                        Minimum 200 responses per non-200 response to require
                        during --test-period, default is 1000 (0.1% errors)
  --min-throughput=MIN_THROUGHPUT
                        Minimum 200 responses per second to require during
                        --test-period, default is 15000
  --test-period=TEST_PERIOD
                        Number of seconds to evaluate after warmup, default is
                        1800 (30 minutes)
  --warmup-threshold=WARMUP_THRESHOLD
                        Minimum 200 responses per second that indicate warmup
                        is complete, default is 15000
  --warmup-timeout=WARMUP_TIMEOUT
                        Maximum number of seconds to wait for warmup to
                        complete, default is 600 (10 minutes)
  --cluster=CLUSTER     Name of GKE cluster to create for test resources,
                        default is 'load-test', ignored when --load-balancer
                        and --no-traffic-generator are both specified
  --location=LOCATION   Location to use for --cluster, default is us-west1
  --preemptible         Use preemptible instances for --cluster, default is
                        False
  --project=PROJECT     Project to use for --cluster, default is from
                        credentials
  --load-balancer=LOAD_BALANCER
                        Load Balancing url map to monitor, implies --no-
                        generator when --server-uri is not specified, ignores
                        --image and --no-emulator
  --server-uri=SERVER_URI
                        Server uri like 'https://edge.stage.domain.com/submit/
                        telemetry/suffix', ignored when --no-generator is
                        specified or --load-balancer is missing
  --image=IMAGE         Docker image for server deployment, default is
                        'mozilla/ingestion-edge:latest', ignored when --load-
                        balancer is specified
  --no-emulator         Don't use a PubSub emulator, ignored when --load-
                        balancer is specified
  --topic=TOPIC         PubSub topic name, default is 'topic', ignored when
                        --load-balancer is specified
  --no-generator        Don't deploy a traffic generator, ignore --script
  --script=SCRIPT       Lua script to use for traffic generator deployment,
                        default is 'tests/load/wrk/telemetry.lua', ignored
                        when --no-generator is specified
```

